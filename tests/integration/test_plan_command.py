from __future__ import annotations

import contextlib
import json
import io
from pathlib import Path

from filescan.cli import main
from tests.helpers.tree_builder import build_tree


def test_plan_command_writes_reviewable_json_artifact(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "project/readme.txt": b"readme-data-1234",
            "project/sub/shared.txt": b"same-data-1234",
            "project_backup/readme.txt": b"readme-data-1234",
            "project_backup/sub/shared.txt": b"same-data-1234",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=8, similarity_threshold=0.2, merge_threshold=0.9)

    assert main(["--config", str(config_path), "scan"]) == 0
    assert main(["--config", str(config_path), "duplicates"]) == 0
    assert main(["--config", str(config_path), "similarity"]) == 0
    assert main(["--config", str(config_path), "plan"]) == 0

    artifact_files = sorted((tmp_path / "filescan").glob("plan-*.json"))
    payload = json.loads(artifact_files[-1].read_text())
    assert payload["proposals"]
    assert {"proposal_id", "action", "source_paths", "target_path", "reason", "approval_state"} <= set(payload["proposals"][0])
    assert (root / "project").exists()
    assert (root / "project_backup").exists()


def test_plan_command_reuses_current_artifact_by_default(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "project/readme.txt": b"readme-data-1234",
            "project_backup/readme.txt": b"readme-data-1234",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=1, similarity_threshold=0.2, merge_threshold=0.9)

    assert main(["--config", str(config_path), "scan"]) == 0
    assert main(["--config", str(config_path), "duplicates"]) == 0
    assert main(["--config", str(config_path), "similarity"]) == 0
    assert main(["--config", str(config_path), "plan"]) == 0
    first_artifact = sorted((tmp_path / "filescan").glob("plan-*.json"))[-1]

    assert main(["--config", str(config_path), "plan"]) == 0

    artifact_files = sorted((tmp_path / "filescan").glob("plan-*.json"))
    assert artifact_files == [first_artifact]


def test_plan_command_recreates_artifact_when_replan_is_requested(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "project/readme.txt": b"readme-data-1234",
            "project_backup/readme.txt": b"readme-data-1234",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=1, similarity_threshold=0.2, merge_threshold=0.9)

    assert main(["--config", str(config_path), "scan"]) == 0
    assert main(["--config", str(config_path), "duplicates"]) == 0
    assert main(["--config", str(config_path), "similarity"]) == 0
    assert main(["--config", str(config_path), "plan"]) == 0
    first_artifact = sorted((tmp_path / "filescan").glob("plan-*.json"))[-1]

    assert main(["--config", str(config_path), "plan", "--replan"]) == 0

    artifact_files = sorted((tmp_path / "filescan").glob("plan-*.json"))
    assert len(artifact_files) == 2
    assert first_artifact in artifact_files
    assert any(path != first_artifact for path in artifact_files)


def test_cli_without_command_runs_default_pipeline(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "project/readme.txt": b"readme-data-1234",
            "project_backup/readme.txt": b"readme-data-1234",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=1, similarity_threshold=0.2, merge_threshold=0.9)

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        assert main(["--config", str(config_path)]) == 0

    artifact_files = sorted((tmp_path / "filescan").glob("plan-*.json"))
    assert artifact_files
    output = stdout.getvalue()
    assert "Run summary:" in output
    assert "scan: completed" in output
    assert "report: completed" in output
    assert str(artifact_files[-1]) in output
    assert (tmp_path / "filescan" / "filescan_report.xlsx").exists()
