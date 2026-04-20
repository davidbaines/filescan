from __future__ import annotations

import json
from pathlib import Path

from filescan.execution.mover import ExecutionRunner
from filescan.models import ScanConfig
from filescan.planning.artifacts import write_plan_artifact
from tests.helpers.tree_builder import build_tree


def _config(tmp_path: Path) -> ScanConfig:
    return ScanConfig(
        roots=[tmp_path],
        filescan_folder=tmp_path / "filescan",
        database_path=tmp_path / "filescan" / "db.sqlite",
        report_path=tmp_path / "filescan" / "report.xlsx",
    )


def _cluster(
    master_path: Path,
    copies: list[tuple[Path, list[Path]]],
    *,
    cluster_id: str = "cluster-0001",
    status: str = "approved",
) -> dict:
    members = [
        {
            "path": str(master_path),
            "is_master": True,
            "file_count": 1,
            "total_bytes": 0,
            "unique_file_count": 0,
            "unique_files": [],
        }
    ]
    for copy_path, unique_files in copies:
        members.append({
            "path": str(copy_path),
            "is_master": False,
            "file_count": 1,
            "total_bytes": 0,
            "unique_file_count": len(unique_files),
            "unique_files": [str(f) for f in unique_files],
        })
    return {
        "cluster_id": cluster_id,
        "min_score": 0.85,
        "is_suppressed": False,
        "status": status,
        "members": members,
    }


def _plan_with_cluster(artifact_dir: Path, cluster: dict) -> Path:
    plan_path = artifact_dir / "plan-test.json"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps({
        "plan_id": "test-plan",
        "created_at": "2026-01-01T00:00:00+00:00",
        "scan_run_id": 1,
        "similarity_scan_run_id": 1,
        "proposals": [],
        "clusters": [cluster],
    }))
    return plan_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_execute_cluster_copies_unique_files_to_master(tmp_path: Path) -> None:
    master = tmp_path / "master"
    copy = tmp_path / "copy"
    unique_file = copy / "unique.txt"
    master.mkdir()
    copy.mkdir()
    unique_file.write_bytes(b"unique content")

    plan_path = _plan_with_cluster(
        tmp_path / "filescan",
        _cluster(master, [(copy, [unique_file])]),
    )

    runner = ExecutionRunner(_config(tmp_path))
    runner.run(plan_path)

    assert (master / "unique.txt").read_bytes() == b"unique content"
    assert unique_file.exists()  # source untouched


def test_execute_cluster_preserves_relative_subdirectory(tmp_path: Path) -> None:
    master = tmp_path / "master"
    copy = tmp_path / "copy"
    unique_file = copy / "sub" / "deep.txt"
    master.mkdir()
    (copy / "sub").mkdir(parents=True)
    unique_file.write_bytes(b"deep file")

    plan_path = _plan_with_cluster(
        tmp_path / "filescan",
        _cluster(master, [(copy, [unique_file])]),
    )

    ExecutionRunner(_config(tmp_path)).run(plan_path)

    assert (master / "sub" / "deep.txt").read_bytes() == b"deep file"


def test_execute_cluster_writes_merged_marker(tmp_path: Path) -> None:
    master = tmp_path / "master"
    copy = tmp_path / "copy"
    master.mkdir()
    copy.mkdir()

    plan_path = _plan_with_cluster(
        tmp_path / "filescan",
        _cluster(master, [(copy, [])]),  # no unique files
    )

    ExecutionRunner(_config(tmp_path)).run(plan_path)

    marker = copy / ".filescan-merged"
    assert marker.exists()
    assert str(master) in marker.read_text()


def test_execute_cluster_result_recorded_in_artifact(tmp_path: Path) -> None:
    master = tmp_path / "master"
    copy = tmp_path / "copy"
    unique_file = copy / "file.txt"
    master.mkdir()
    copy.mkdir()
    unique_file.write_bytes(b"data")

    plan_path = _plan_with_cluster(
        tmp_path / "filescan",
        _cluster(master, [(copy, [unique_file])]),
    )

    artifact_path = ExecutionRunner(_config(tmp_path)).run(plan_path)
    payload = json.loads(artifact_path.read_text())
    results = payload["proposal_results"]

    assert len(results) == 1
    assert results[0]["proposal_id"] == "cluster-0001"
    assert results[0]["verification_passed"] is True
    assert results[0]["source_cleanup_allowed"] is True
    assert results[0]["files_copied"] == 1


def test_execute_cluster_skips_when_not_approved(tmp_path: Path) -> None:
    master = tmp_path / "master"
    copy = tmp_path / "copy"
    unique_file = copy / "file.txt"
    master.mkdir()
    copy.mkdir()
    unique_file.write_bytes(b"data")

    plan_path = _plan_with_cluster(
        tmp_path / "filescan",
        _cluster(master, [(copy, [unique_file])], status="pending"),
    )

    ExecutionRunner(_config(tmp_path)).run(plan_path)

    assert not (master / "file.txt").exists()


def test_execute_cluster_conflict_sets_verification_failed(tmp_path: Path) -> None:
    master = tmp_path / "master"
    copy = tmp_path / "copy"
    unique_file = copy / "conflict.txt"
    master.mkdir()
    copy.mkdir()
    unique_file.write_bytes(b"copy version")
    (master / "conflict.txt").write_bytes(b"different master version")

    plan_path = _plan_with_cluster(
        tmp_path / "filescan",
        _cluster(master, [(copy, [unique_file])]),
    )

    artifact_path = ExecutionRunner(_config(tmp_path)).run(plan_path)
    payload = json.loads(artifact_path.read_text())
    result = payload["proposal_results"][0]

    assert result["verification_passed"] is False
    assert result["source_cleanup_allowed"] is False
    assert result["errors"]
