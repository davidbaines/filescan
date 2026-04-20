from __future__ import annotations

import json
from pathlib import Path

from filescan.cli import main
from filescan.planning.artifacts import write_plan_artifact
from filescan.models import PlanProposal
from tests.helpers.tree_builder import build_tree


def test_execute_verify_copies_files_and_keeps_source_until_cleanup_is_explicit(tmp_path: Path, write_config) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    build_tree(source, {"nested/file.txt": b"payload-1234"})
    config_path = write_config([source])
    plan_path = write_plan_artifact(
        tmp_path / "filescan",
        [
            PlanProposal(
                proposal_id="proposal-0001",
                action="merge_folder",
                source_paths=(source,),
                target_path=target,
                evidence=("manual:test",),
                reason="manual execution test",
                approval_state="approved",
            )
        ],
    )

    assert main(["--config", str(config_path), "execute", "--plan", str(plan_path)]) == 0

    artifact_files = sorted((tmp_path / "filescan").glob("execution-*.json"))
    payload = json.loads(artifact_files[-1].read_text())
    result = payload["proposal_results"][0]
    assert (target / "nested/file.txt").read_bytes() == b"payload-1234"
    assert source.exists()
    assert result["verification_passed"] is True
    assert result["source_cleanup_allowed"] is True


def test_execute_verify_fails_when_target_has_conflicting_content(tmp_path: Path, write_config) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    build_tree(source, {"nested/file.txt": b"payload-1234"})
    build_tree(target, {"nested/file.txt": b"conflicting-data"})
    config_path = write_config([source])
    plan_path = write_plan_artifact(
        tmp_path / "filescan",
        [
            PlanProposal(
                proposal_id="proposal-0001",
                action="merge_folder",
                source_paths=(source,),
                target_path=target,
                evidence=("manual:test",),
                reason="conflict execution test",
                approval_state="approved",
            )
        ],
    )

    assert main(["--config", str(config_path), "execute", "--plan", str(plan_path)]) == 0

    artifact_files = sorted((tmp_path / "filescan").glob("execution-*.json"))
    payload = json.loads(artifact_files[-1].read_text())
    result = payload["proposal_results"][0]
    assert source.exists()
    assert result["verification_passed"] is False
    assert result["source_cleanup_allowed"] is False
    assert result["errors"]
