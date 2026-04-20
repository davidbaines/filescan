from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from filescan.models import ClusterMember, FolderCluster, ScanConfig
from filescan.storage import FileRepository, SQLiteDB


class _UnionFind:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def groups(self) -> list[set[str]]:
        result: dict[str, set[str]] = defaultdict(set)
        for x in list(self._parent.keys()):
            result[self.find(x)].add(x)
        return list(result.values())


def _select_master(paths: list[Path], config: ScanConfig, db: SQLiteDB) -> Path:
    """Pick the master: prefer earliest config root, then highest file count, shallowest depth, newest mtime."""
    paths_in_root: list[Path] = []
    for root in config.roots:
        root_str = str(root).rstrip("\\")
        matched = [p for p in paths if str(p).startswith(root_str)]
        if matched:
            paths_in_root = matched
            break
    if not paths_in_root:
        paths_in_root = paths

    if len(paths_in_root) == 1:
        return paths_in_root[0]

    placeholders = ",".join("?" * len(paths_in_root))
    rows = db.conn.execute(
        f"SELECT path, file_count, depth, mtime FROM folders WHERE path IN ({placeholders}) AND is_missing = 0",
        [str(p) for p in paths_in_root],
    ).fetchall()

    if not rows:
        return min(paths_in_root, key=str)

    stats = {str(row["path"]): row for row in rows}

    def _key(p: Path) -> tuple[int, int, float]:
        row = stats.get(str(p))
        if row is None:
            return (0, 9999, 0.0)
        return (-int(row["file_count"]), int(row["depth"]), -(float(row["mtime"] or 0.0)))

    return min(paths_in_root, key=_key)


def _find_unique_files(db: SQLiteDB, folder_path: Path, master_path: Path) -> list[Path]:
    """Return paths of files under folder_path not present (by full_hash) in master_path."""
    folder_str = str(folder_path)
    folder_like = folder_str.rstrip("\\") + "\\%"
    master_str = str(master_path)
    master_like = master_str.rstrip("\\") + "\\%"

    rows = db.conn.execute(
        """
        SELECT f.path
        FROM files f
        JOIN folders fo ON fo.id = f.folder_id
        LEFT JOIN file_hashes fh ON fh.file_id = f.id
        WHERE (fo.path = ? OR fo.path LIKE ?)
          AND f.is_missing = 0
          AND fo.is_missing = 0
          AND (
            fh.full_hash IS NULL
            OR fh.full_hash NOT IN (
              SELECT fh2.full_hash
              FROM files f2
              JOIN folders fo2 ON fo2.id = f2.folder_id
              LEFT JOIN file_hashes fh2 ON fh2.file_id = f2.id
              WHERE (fo2.path = ? OR fo2.path LIKE ?)
                AND f2.is_missing = 0
                AND fo2.is_missing = 0
                AND fh2.full_hash IS NOT NULL
            )
          )
        ORDER BY f.path
        """,
        (folder_str, folder_like, master_str, master_like),
    ).fetchall()
    return [Path(row["path"]) for row in rows]


def recompute_unique_files(cluster: FolderCluster, new_master_path: Path, db: SQLiteDB) -> FolderCluster:
    """Return a new FolderCluster with master switched to new_master_path and unique files recomputed."""
    new_members: list[ClusterMember] = []
    for m in cluster.members:
        is_master = m.path == new_master_path
        unique: tuple[Path, ...] = () if is_master else tuple(_find_unique_files(db, m.path, new_master_path))
        new_members.append(ClusterMember(
            path=m.path,
            is_master=is_master,
            file_count=m.file_count,
            total_bytes=m.total_bytes,
            unique_file_paths=unique,
        ))
    return FolderCluster(
        cluster_id=cluster.cluster_id,
        members=tuple(new_members),
        min_score=cluster.min_score,
        is_suppressed=cluster.is_suppressed,
    )


def _apply_hierarchy_suppression(clusters: list[FolderCluster]) -> None:
    """Suppress C2 only if every member of C2 has an ancestor in one single parent cluster C1.

    The single-cluster requirement prevents false suppression when ancestors span
    multiple unrelated clusters — a looser 'any ancestor in any cluster' rule would
    silence valid findings.
    """
    active: list[frozenset[Path]] = []

    for cluster in clusters:
        member_paths = frozenset(m.path for m in cluster.members)
        covering: frozenset[Path] | None = None

        for active_paths in active:
            if all(
                any(ancestor in active_paths for ancestor in p.parents)
                for p in member_paths
            ):
                covering = active_paths
                break

        if covering is not None:
            cluster.is_suppressed = True
        else:
            active.append(member_paths)


def build_clusters(config: ScanConfig, db: SQLiteDB) -> list[FolderCluster]:
    """Build folder clusters from similarity candidates.

    Three-track dispatch (same as build_plan_artifact):
      - score >= merge_threshold AND name_bonus > 0  → mark_backup (excluded from clusters)
      - score >= similarity_cluster_threshold         → merge_cluster (this function)
      - score <  similarity_cluster_threshold         → needs_review (not handled here)
    """
    repo = FileRepository(db)
    candidates = repo.list_similarity_candidates()

    threshold = config.similarity_cluster_threshold
    merge_threshold = config.merge_threshold

    mark_backup_pairs: set[tuple[str, str]] = set()
    cluster_candidates = []
    for c in candidates:
        key = (min(str(c.folder_a), str(c.folder_b)), max(str(c.folder_a), str(c.folder_b)))
        if c.score >= merge_threshold and c.name_bonus > 0:
            mark_backup_pairs.add(key)
        elif c.score >= threshold:
            cluster_candidates.append(c)

    if not cluster_candidates:
        return []

    uf = _UnionFind()
    pair_scores: dict[tuple[str, str], float] = {}
    for c in cluster_candidates:
        key = (min(str(c.folder_a), str(c.folder_b)), max(str(c.folder_a), str(c.folder_b)))
        uf.union(str(c.folder_a), str(c.folder_b))
        pair_scores[key] = max(pair_scores.get(key, 0.0), c.score)

    groups = [g for g in uf.groups() if len(g) >= 2]
    if not groups:
        return []

    clusters: list[FolderCluster] = []
    for i, group in enumerate(sorted(groups, key=lambda g: sorted(g)), start=1):
        paths = sorted([Path(p) for p in group], key=str)
        path_strs = {str(p) for p in paths}

        relevant_scores = [
            score
            for (a, b), score in pair_scores.items()
            if a in path_strs and b in path_strs
        ]
        min_score = min(relevant_scores) if relevant_scores else threshold

        master_path = _select_master(paths, config, db)

        members: list[ClusterMember] = []
        for path in paths:
            row = db.conn.execute(
                "SELECT file_count, total_bytes FROM folders WHERE path = ? AND is_missing = 0",
                (str(path),),
            ).fetchone()
            file_count = int(row["file_count"]) if row else 0
            total_bytes = int(row["total_bytes"]) if row else 0
            is_master = path == master_path
            unique: tuple[Path, ...] = () if is_master else tuple(_find_unique_files(db, path, master_path))
            members.append(ClusterMember(
                path=path,
                is_master=is_master,
                file_count=file_count,
                total_bytes=total_bytes,
                unique_file_paths=unique,
            ))

        clusters.append(FolderCluster(
            cluster_id=f"cluster-{i:04d}",
            members=tuple(members),
            min_score=min_score,
        ))

    clusters.sort(key=lambda c: min(len(m.path.parts) for m in c.members))
    _apply_hierarchy_suppression(clusters)
    return clusters


def _cycle_master_in_dict(cluster_dict: dict, db: SQLiteDB) -> dict:
    """Cycle the master to the next member in-place and recompute unique files.

    Mutates and returns cluster_dict so callers can chain or ignore the return value.
    """
    members = cluster_dict["members"]
    current_idx = next((i for i, m in enumerate(members) if m["is_master"]), 0)
    next_idx = (current_idx + 1) % len(members)
    new_master_path = Path(members[next_idx]["path"])

    fc = FolderCluster(
        cluster_id=cluster_dict["cluster_id"],
        members=tuple(
            ClusterMember(
                path=Path(m["path"]),
                is_master=m["is_master"],
                file_count=m["file_count"],
                total_bytes=m["total_bytes"],
                unique_file_paths=tuple(Path(p) for p in m["unique_files"]),
            )
            for m in members
        ),
        min_score=cluster_dict["min_score"],
        is_suppressed=cluster_dict["is_suppressed"],
    )

    updated = recompute_unique_files(fc, new_master_path, db)
    cluster_dict["members"] = [
        {
            "path": str(m.path),
            "is_master": m.is_master,
            "file_count": m.file_count,
            "total_bytes": m.total_bytes,
            "unique_file_count": len(m.unique_file_paths),
            "unique_files": [str(p) for p in m.unique_file_paths],
        }
        for m in updated.members
    ]
    return cluster_dict


def find_clusters(config_path: Path) -> list[FolderCluster]:
    from filescan.config import load_config
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    clusters = build_clusters(config, db)
    db.close()
    return clusters


def dump_clusters_json(clusters: list[FolderCluster]) -> str:
    def _member(m: ClusterMember) -> dict:
        return {
            "path": str(m.path),
            "is_master": m.is_master,
            "file_count": m.file_count,
            "total_bytes": m.total_bytes,
            "unique_file_count": len(m.unique_file_paths),
            "unique_files": [str(p) for p in m.unique_file_paths],
        }

    return json.dumps(
        [
            {
                "cluster_id": c.cluster_id,
                "min_score": round(c.min_score, 4),
                "is_suppressed": c.is_suppressed,
                "total_bytes": c.total_bytes,
                "members": [_member(m) for m in c.members],
            }
            for c in clusters
        ],
        indent=2,
    )
