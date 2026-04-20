from __future__ import annotations

import json
from pathlib import Path

from filescan.models import FolderRecord, FolderSimilarityCandidate, ScanConfig
from filescan.planning.proposals import _build_non_cluster_proposals, build_plan_artifact
from filescan.planning.artifacts import load_plan_artifact
from filescan.storage import FileRepository, SQLiteDB


def _base_config(tmp_path: Path, db_path: Path) -> ScanConfig:
    return ScanConfig(
        roots=[tmp_path],
        filescan_folder=tmp_path / "filescan",
        database_path=db_path,
        report_path=tmp_path / "report.xlsx",
        similarity_threshold=0.2,
        similarity_cluster_threshold=0.70,
        merge_threshold=0.93,
    )


def _setup(tmp_path: Path) -> tuple[SQLiteDB, FileRepository, int, ScanConfig]:
    db_path = tmp_path / "test.db"
    db = SQLiteDB(db_path)
    repo = FileRepository(db)
    run_id = repo.begin_scan_run()
    config = _base_config(tmp_path, db_path)
    return db, repo, run_id, config


def _add_folder(repo: FileRepository, path: Path, run_id: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    repo.upsert_folder(FolderRecord(
        path=path, drive=path.drive, parent_path=path.parent,
        depth=1, file_count=5, total_bytes=500_000,
        mtime=path.stat().st_mtime, scan_run_id=run_id,
    ))


def test_non_cluster_proposals_mark_backup_excluded_from_cluster_track(tmp_path: Path) -> None:
    db, repo, run_id, config = _setup(tmp_path)
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

    proposals = _build_non_cluster_proposals(config, repo)
    db.close()

    assert len(proposals) == 1
    assert proposals[0].action == "mark_backup"
    assert proposals[0].target_path == folder_a


def test_non_cluster_proposals_low_score_becomes_needs_review(tmp_path: Path) -> None:
    db, repo, run_id, config = _setup(tmp_path)
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

    proposals = _build_non_cluster_proposals(config, repo)
    db.close()

    assert len(proposals) == 1
    assert proposals[0].action == "needs_review"
    assert proposals[0].target_path is None


def test_non_cluster_proposals_cluster_eligible_pair_skipped(tmp_path: Path) -> None:
    db, repo, run_id, config = _setup(tmp_path)
    folder_a = tmp_path / "A"
    folder_b = tmp_path / "B"
    for f in (folder_a, folder_b):
        _add_folder(repo, f, run_id)

    repo.replace_similarity_candidates([
        FolderSimilarityCandidate(
            folder_a=folder_a, folder_b=folder_b,
            folder_a_id=repo.get_folder_id(folder_a),
            folder_b_id=repo.get_folder_id(folder_b),
            score=0.80, shared_duplicate_files=4, shared_signatures=4,
            name_bonus=0.0, size_ratio=1.0, file_count_ratio=1.0, reason="strong",
        ),
    ])

    proposals = _build_non_cluster_proposals(config, repo)
    db.close()

    assert proposals == []


def test_build_plan_artifact_writes_clusters_and_proposals(tmp_path: Path, write_config: object) -> None:
    db_path = tmp_path / "filescan" / "file_index.db"
    config_path = tmp_path / "config.yml"

    import yaml
    from filescan.inventory.normalizer import normalize_path
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(normalize_path(tmp_path))}],
        "filescan_folder": str(tmp_path / "filescan"),
        "database_folder": str(tmp_path / "filescan"),
        "database_filename": "file_index.db",
        "report_filename": "report.xlsx",
        "similarity_cluster_threshold": 0.70,
        "merge_threshold": 0.93,
    }))

    db = SQLiteDB(db_path)
    repo = FileRepository(db)
    run_id = repo.begin_scan_run()
    repo.set_stage_scan_run_id("similarity", run_id)

    # One backup pair (→ mark_backup proposal) and one cluster pair (→ cluster)
    folder_proj = tmp_path / "project"
    folder_back = tmp_path / "project_backup"
    folder_a = tmp_path / "A"
    folder_b = tmp_path / "B"
    for f in (folder_proj, folder_back, folder_a, folder_b):
        _add_folder(repo, f, run_id)

    repo.replace_similarity_candidates([
        FolderSimilarityCandidate(
            folder_a=folder_proj, folder_b=folder_back,
            folder_a_id=repo.get_folder_id(folder_proj),
            folder_b_id=repo.get_folder_id(folder_back),
            score=0.95, shared_duplicate_files=5, shared_signatures=5,
            name_bonus=0.2, size_ratio=1.0, file_count_ratio=1.0, reason="backup",
        ),
        FolderSimilarityCandidate(
            folder_a=folder_a, folder_b=folder_b,
            folder_a_id=repo.get_folder_id(folder_a),
            folder_b_id=repo.get_folder_id(folder_b),
            score=0.82, shared_duplicate_files=4, shared_signatures=4,
            name_bonus=0.0, size_ratio=1.0, file_count_ratio=1.0, reason="strong",
        ),
    ])
    db.close()

    artifact_path = build_plan_artifact(config_path, replan=True)
    payload = load_plan_artifact(artifact_path)

    proposals = payload["proposals"]
    clusters = payload["clusters"]

    assert len(proposals) == 1
    assert proposals[0]["action"] == "mark_backup"

    assert len(clusters) == 1
    cluster = clusters[0]
    assert cluster["status"] == "pending"
    member_paths = {m["path"] for m in cluster["members"]}
    assert member_paths == {str(folder_a), str(folder_b)}
    assert any(m["is_master"] for m in cluster["members"])
