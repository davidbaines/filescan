from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from filescan.models import ExecutionResult, PlanProposal


def latest_plan_artifact(artifact_dir: Path) -> Path | None:
    matches = sorted(artifact_dir.glob("plan-*.json"))
    if not matches:
        return None
    return matches[-1]


def write_plan_artifact(
    artifact_dir: Path,
    proposals: list[PlanProposal],
    *,
    scan_run_id: int | None = None,
    similarity_scan_run_id: int | None = None,
) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    plan_id = uuid4().hex
    artifact_path = artifact_dir / f"plan-{plan_id}.json"
    payload = {
        "plan_id": plan_id,
        "created_at": datetime.now(UTC).isoformat(),
        "scan_run_id": scan_run_id,
        "similarity_scan_run_id": similarity_scan_run_id,
        "proposals": [
            {
                "proposal_id": proposal.proposal_id,
                "action": proposal.action,
                "source_paths": [str(path) for path in proposal.source_paths],
                "target_path": None if proposal.target_path is None else str(proposal.target_path),
                "evidence": list(proposal.evidence),
                "reason": proposal.reason,
                "approval_state": proposal.approval_state,
            }
            for proposal in proposals
        ],
    }
    artifact_path.write_text(json.dumps(payload, indent=2))
    return artifact_path


def load_plan_artifact(plan_path: Path) -> dict[str, object]:
    return json.loads(plan_path.read_text())


def write_execution_artifact(artifact_dir: Path, *, plan_id: str, results: list[ExecutionResult]) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    execution_id = uuid4().hex
    artifact_path = artifact_dir / f"execution-{execution_id}.json"
    payload = {
        "execution_id": execution_id,
        "plan_id": plan_id,
        "created_at": datetime.now(UTC).isoformat(),
        "proposal_results": [
            {
                **asdict(result),
                "errors": list(result.errors),
            }
            for result in results
        ],
    }
    artifact_path.write_text(json.dumps(payload, indent=2))
    return artifact_path
