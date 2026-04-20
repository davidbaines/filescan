from __future__ import annotations

from pathlib import Path

from filescan.config import load_config
from filescan.models import PlanProposal, ScanConfig
from filescan.planning.artifacts import latest_plan_artifact, load_plan_artifact, write_plan_artifact
from filescan.similarity.clusters import build_clusters
from filescan.storage import FileRepository, SQLiteDB


class ProposalBuilder:
    def __init__(self, config: ScanConfig) -> None:
        self.config = config

    def _is_backup_name(self, path: Path) -> bool:
        lower_name = path.name.lower()
        return any(token in lower_name for token in self.config.backup_name_tokens)

    def _canonical_pair(self, first: Path, second: Path) -> tuple[Path, Path]:
        first_backup = self._is_backup_name(first)
        second_backup = self._is_backup_name(second)
        if first_backup and not second_backup:
            return second, first
        if second_backup and not first_backup:
            return first, second
        ordered = sorted((first, second), key=lambda item: (len(item.parts), str(item)))
        return ordered[0], ordered[1]

    def build(self) -> list[PlanProposal]:
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        candidates = repo.list_similarity_candidates()
        proposals: list[PlanProposal] = []
        for index, candidate in enumerate(candidates, start=1):
            canonical, other = self._canonical_pair(candidate.folder_a, candidate.folder_b)
            evidence = (f"similarity:{candidate.id or index}", candidate.reason)
            if candidate.score >= self.config.merge_threshold and candidate.name_bonus > 0:
                action = "mark_backup"
                sources = (other,)
                target = canonical
                reason = f"Folder looks like a backup copy of {canonical} based on {candidate.reason}."
            elif candidate.score >= self.config.merge_threshold:
                action = "merge_folder"
                sources = (other,)
                target = canonical
                reason = f"Folder contents strongly overlap and should be merged into {canonical}."
            else:
                action = "needs_review"
                sources = tuple(sorted((candidate.folder_a, candidate.folder_b), key=str))
                target = None
                reason = f"Folder pair needs review before any move because evidence is mixed: {candidate.reason}."
            proposals.append(
                PlanProposal(
                    proposal_id=f"proposal-{index:04d}",
                    action=action,
                    source_paths=sources,
                    target_path=target,
                    evidence=evidence,
                    reason=reason,
                )
            )
        db.close()
        return proposals


def build_proposals(config_path: str | Path) -> list[PlanProposal]:
    return ProposalBuilder(load_config(config_path)).build()


def _build_non_cluster_proposals(
    config: ScanConfig, repo: FileRepository
) -> list[PlanProposal]:
    """Generate mark_backup and needs_review proposals (three-track dispatch, tracks 1 and 3).

    Track 2 (merge_cluster) is handled separately by build_clusters().
    """
    builder = ProposalBuilder(config)
    candidates = repo.list_similarity_candidates()
    proposals: list[PlanProposal] = []
    proposal_index = 0

    for candidate in candidates:
        is_backup = candidate.score >= config.merge_threshold and candidate.name_bonus > 0
        is_cluster_eligible = candidate.score >= config.similarity_cluster_threshold and not is_backup

        if is_cluster_eligible:
            continue  # handled by build_clusters

        proposal_index += 1
        pid = f"proposal-{proposal_index:04d}"
        evidence = (f"similarity:{candidate.id or proposal_index}", candidate.reason)

        if is_backup:
            canonical, other = builder._canonical_pair(candidate.folder_a, candidate.folder_b)
            proposals.append(PlanProposal(
                proposal_id=pid,
                action="mark_backup",
                source_paths=(other,),
                target_path=canonical,
                evidence=evidence,
                reason=f"Folder looks like a backup copy of {canonical} based on {candidate.reason}.",
            ))
        else:
            sources = tuple(sorted((candidate.folder_a, candidate.folder_b), key=str))
            proposals.append(PlanProposal(
                proposal_id=pid,
                action="needs_review",
                source_paths=sources,
                target_path=None,
                evidence=evidence,
                reason=f"Folder pair needs review before any move because evidence is mixed: {candidate.reason}.",
            ))

    return proposals


def build_plan_artifact(config_path: str | Path, *, replan: bool = False) -> Path:
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    latest_scan_run_id = repo.latest_scan_run_id()
    similarity_scan_run_id = repo.get_stage_scan_run_id("similarity")
    latest_artifact_path = latest_plan_artifact(config.artifact_dir)

    if not replan and latest_artifact_path is not None:
        payload = load_plan_artifact(latest_artifact_path)
        if int(payload.get("scan_run_id") or 0) >= latest_scan_run_id and int(payload.get("similarity_scan_run_id") or 0) >= similarity_scan_run_id:
            repo.set_stage_scan_run_id("plan", latest_scan_run_id)
            print("Skipping plan generation without --replan; existing artifact is current.")
            db.close()
            return latest_artifact_path

    proposals = _build_non_cluster_proposals(config, repo)
    clusters = build_clusters(config, db)

    artifact_path = write_plan_artifact(
        config.artifact_dir,
        proposals,
        clusters=clusters,
        scan_run_id=latest_scan_run_id,
        similarity_scan_run_id=similarity_scan_run_id,
    )
    repo.set_stage_scan_run_id("plan", latest_scan_run_id)
    db.close()
    return artifact_path
