from __future__ import annotations

from pathlib import Path

import pytest

from filescan.models import (
    ClusterMember,
    FileRecord,
    FolderCluster,
    FolderRecord,
    FolderSimilarityCandidate,
    ScanConfig,
)
from filescan.similarity.clusters import (
    _UnionFind,
    _apply_hierarchy_suppression,
    _find_unique_files,
    build_clusters,
)
from filescan.storage import FileRepository, SQLiteDB


# ---------------------------------------------------------------------------
# _UnionFind unit tests
# ---------------------------------------------------------------------------


def test_union_find_connects_transitive_pairs() -> None:
    uf = _UnionFind()
    uf.union("A", "B")
    uf.union("B", "C")
    groups = uf.groups()
    assert len(groups) == 1
    assert groups[0] == {"A", "B", "C"}


def test_union_find_produces_separate_groups() -> None:
    uf = _UnionFind()
    uf.union("A", "B")
    uf.union("C", "D")
    groups = sorted(uf.groups(), key=lambda g: sorted(g)[0])
    assert len(groups) == 2
    assert groups[0] == {"A", "B"}
    assert groups[1] == {"C", "D"}


def test_union_find_single_node_not_grouped() -> None:
    uf = _UnionFind()
    uf.union("A", "B")
    uf.find("C")  # register C without union
    groups = uf.groups()
    single = next(g for g in groups if "C" in g)
    assert single == {"C"}


# ---------------------------------------------------------------------------
# _apply_hierarchy_suppression tests
# ---------------------------------------------------------------------------


def _make_cluster(paths: list[str], min_score: float = 0.8) -> FolderCluster:
    members = tuple(
        ClusterMember(
            path=Path(p),
            is_master=(i == 0),
            file_count=1,
            total_bytes=1000,
            unique_file_paths=(),
        )
        for i, p in enumerate(paths)
    )
    return FolderCluster(cluster_id="x", members=members, min_score=min_score)


def test_hierarchy_suppression_suppresses_all_children_covered_by_one_parent() -> None:
    parent = _make_cluster(["C:/A", "C:/B"])
    child = _make_cluster(["C:/A/child", "C:/B/child"])
    clusters = [parent, child]
    _apply_hierarchy_suppression(clusters)
    assert not parent.is_suppressed
    assert child.is_suppressed


def test_hierarchy_suppression_not_suppressed_when_ancestry_spans_clusters() -> None:
    c1 = _make_cluster(["C:/A", "C:/B"])
    c2 = _make_cluster(["C:/X", "C:/Y"])
    child = _make_cluster(["C:/A/kid", "C:/X/kid"])  # parents in different clusters
    clusters = [c1, c2, child]
    _apply_hierarchy_suppression(clusters)
    assert not child.is_suppressed


def test_hierarchy_suppression_not_suppressed_when_one_member_has_no_ancestor() -> None:
    parent = _make_cluster(["C:/A", "C:/B"])
    # C:/C/kid has parent C:/C which is NOT in parent cluster
    child = _make_cluster(["C:/A/kid", "C:/C/kid"])
    clusters = [parent, child]
    _apply_hierarchy_suppression(clusters)
    assert not child.is_suppressed


# ---------------------------------------------------------------------------
# build_clusters integration tests (using in-memory SQLite)
# ---------------------------------------------------------------------------


def _setup_db(tmp_path: Path) -> tuple[SQLiteDB, FileRepository, int]:
    db = SQLiteDB(tmp_path / "test.db")
    repo = FileRepository(db)
    run_id = repo.begin_scan_run()
    return db, repo, run_id


def _add_folder(repo: FileRepository, path: Path, run_id: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    repo.upsert_folder(
        FolderRecord(
            path=path,
            drive=path.drive,
            parent_path=path.parent,
            depth=len(path.parts) - 1,
            file_count=5,
            total_bytes=500_000,
            mtime=path.stat().st_mtime,
            scan_run_id=run_id,
        )
    )


def test_build_clusters_groups_three_transitively_connected_folders(tmp_path: Path) -> None:
    db, repo, run_id = _setup_db(tmp_path)
    folder_a = tmp_path / "A"
    folder_b = tmp_path / "B"
    folder_c = tmp_path / "C"
    for f in (folder_a, folder_b, folder_c):
        _add_folder(repo, f, run_id)

    repo.replace_similarity_candidates([
        FolderSimilarityCandidate(
            folder_a=folder_a, folder_b=folder_b,
            folder_a_id=repo.get_folder_id(folder_a),
            folder_b_id=repo.get_folder_id(folder_b),
            score=0.85, shared_duplicate_files=3, shared_signatures=3,
            name_bonus=0.0, size_ratio=1.0, file_count_ratio=1.0, reason="shared",
        ),
        FolderSimilarityCandidate(
            folder_a=folder_b, folder_b=folder_c,
            folder_a_id=repo.get_folder_id(folder_b),
            folder_b_id=repo.get_folder_id(folder_c),
            score=0.75, shared_duplicate_files=2, shared_signatures=2,
            name_bonus=0.0, size_ratio=0.9, file_count_ratio=0.9, reason="shared",
        ),
    ])

    config = ScanConfig(
        roots=[tmp_path],
        filescan_folder=tmp_path / "filescan",
        database_path=tmp_path / "test.db",
        report_path=tmp_path / "report.xlsx",
        similarity_cluster_threshold=0.70,
        merge_threshold=0.93,
    )

    clusters = build_clusters(config, db)
    db.close()

    assert len(clusters) == 1
    cluster_paths = {m.path for m in clusters[0].members}
    assert cluster_paths == {folder_a, folder_b, folder_c}
    assert clusters[0].min_score == pytest.approx(0.75)


def test_build_clusters_excludes_mark_backup_pairs(tmp_path: Path) -> None:
    db, repo, run_id = _setup_db(tmp_path)
    folder_a = tmp_path / "project"
    folder_b = tmp_path / "project_backup"
    for f in (folder_a, folder_b):
        _add_folder(repo, f, run_id)

    repo.replace_similarity_candidates([
        FolderSimilarityCandidate(
            folder_a=folder_a, folder_b=folder_b,
            folder_a_id=repo.get_folder_id(folder_a),
            folder_b_id=repo.get_folder_id(folder_b),
            score=0.95, shared_duplicate_files=5, shared_signatures=5,
            name_bonus=0.2, size_ratio=1.0, file_count_ratio=1.0, reason="backup name",
        ),
    ])

    config = ScanConfig(
        roots=[tmp_path],
        filescan_folder=tmp_path / "filescan",
        database_path=tmp_path / "test.db",
        report_path=tmp_path / "report.xlsx",
        similarity_cluster_threshold=0.70,
        merge_threshold=0.93,
    )

    clusters = build_clusters(config, db)
    db.close()

    assert clusters == []


def test_build_clusters_empty_when_all_pairs_below_threshold(tmp_path: Path) -> None:
    db, repo, run_id = _setup_db(tmp_path)
    folder_a = tmp_path / "A"
    folder_b = tmp_path / "B"
    for f in (folder_a, folder_b):
        _add_folder(repo, f, run_id)

    repo.replace_similarity_candidates([
        FolderSimilarityCandidate(
            folder_a=folder_a, folder_b=folder_b,
            folder_a_id=repo.get_folder_id(folder_a),
            folder_b_id=repo.get_folder_id(folder_b),
            score=0.50, shared_duplicate_files=1, shared_signatures=1,
            name_bonus=0.0, size_ratio=0.8, file_count_ratio=0.8, reason="weak",
        ),
    ])

    config = ScanConfig(
        roots=[tmp_path],
        filescan_folder=tmp_path / "filescan",
        database_path=tmp_path / "test.db",
        report_path=tmp_path / "report.xlsx",
        similarity_cluster_threshold=0.70,
        merge_threshold=0.93,
    )

    clusters = build_clusters(config, db)
    db.close()

    assert clusters == []


# ---------------------------------------------------------------------------
# _find_unique_files tests
# ---------------------------------------------------------------------------


def test_find_unique_files_returns_files_not_in_master(tmp_path: Path) -> None:
    db = SQLiteDB(tmp_path / "test.db")
    repo = FileRepository(db)
    run_id = repo.begin_scan_run()

    master_dir = tmp_path / "master"
    copy_dir = tmp_path / "copy"
    for d in (master_dir, copy_dir):
        d.mkdir()
        repo.upsert_folder(
            FolderRecord(
                path=d, drive=d.drive, parent_path=d.parent,
                depth=1, file_count=0, total_bytes=0,
                mtime=d.stat().st_mtime, scan_run_id=run_id,
            )
        )

    master_folder_id = repo.get_folder_id(master_dir)
    copy_folder_id = repo.get_folder_id(copy_dir)
    assert master_folder_id is not None
    assert copy_folder_id is not None

    shared_hash = "aabbcc"
    unique_hash = "112233"

    # Insert shared file in master
    db.conn.execute(
        "INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id) VALUES (?,?,?,?,?,?,?)",
        (master_folder_id, "shared.txt", str(master_dir / "shared.txt"), 100, 0.0, 0.0, run_id),
    )
    master_file_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.conn.execute(
        "INSERT INTO file_hashes (file_id, full_hash) VALUES (?,?)", (master_file_id, shared_hash)
    )

    # Insert shared file in copy
    db.conn.execute(
        "INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id) VALUES (?,?,?,?,?,?,?)",
        (copy_folder_id, "shared.txt", str(copy_dir / "shared.txt"), 100, 0.0, 0.0, run_id),
    )
    copy_shared_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.conn.execute(
        "INSERT INTO file_hashes (file_id, full_hash) VALUES (?,?)", (copy_shared_id, shared_hash)
    )

    # Insert unique file in copy (hash not in master)
    db.conn.execute(
        "INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id) VALUES (?,?,?,?,?,?,?)",
        (copy_folder_id, "unique.txt", str(copy_dir / "unique.txt"), 200, 0.0, 0.0, run_id),
    )
    copy_unique_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.conn.execute(
        "INSERT INTO file_hashes (file_id, full_hash) VALUES (?,?)", (copy_unique_id, unique_hash)
    )
    db.conn.commit()

    result = _find_unique_files(db, copy_dir, master_dir)
    db.close()

    assert result == [copy_dir / "unique.txt"]


def test_find_unique_files_treats_unhashed_files_as_unique(tmp_path: Path) -> None:
    db = SQLiteDB(tmp_path / "test.db")
    repo = FileRepository(db)
    run_id = repo.begin_scan_run()

    master_dir = tmp_path / "master"
    copy_dir = tmp_path / "copy"
    for d in (master_dir, copy_dir):
        d.mkdir()
        repo.upsert_folder(
            FolderRecord(
                path=d, drive=d.drive, parent_path=d.parent,
                depth=1, file_count=0, total_bytes=0,
                mtime=d.stat().st_mtime, scan_run_id=run_id,
            )
        )

    copy_folder_id = repo.get_folder_id(copy_dir)

    # Insert unhashed file in copy
    db.conn.execute(
        "INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id) VALUES (?,?,?,?,?,?,?)",
        (copy_folder_id, "unknown.bin", str(copy_dir / "unknown.bin"), 500, 0.0, 0.0, run_id),
    )
    db.conn.commit()

    result = _find_unique_files(db, copy_dir, master_dir)
    db.close()

    assert copy_dir / "unknown.bin" in result
