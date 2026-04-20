from __future__ import annotations

from pathlib import Path

from filescan.models import FolderRecord, FolderSimilarityCandidate, ScanConfig
from filescan.planning.proposals import ProposalBuilder
from filescan.storage import FileRepository, SQLiteDB


def test_plan_builder_emits_backup_and_review_proposals(tmp_path: Path) -> None:
    db_path = tmp_path / "plans.db"
    db = SQLiteDB(db_path)
    repo = FileRepository(db)
    run_id = repo.begin_scan_run()
    for folder in (tmp_path / "project", tmp_path / "project_backup", tmp_path / "unclear", tmp_path / "unclear_copy"):
        folder.mkdir(parents=True, exist_ok=True)
        repo.upsert_folder(
            FolderRecord(
                path=folder,
                drive=folder.drive,
                parent_path=tmp_path,
                depth=1,
                file_count=0,
                total_bytes=0,
                mtime=folder.stat().st_mtime,
                scan_run_id=run_id,
            )
        )
    repo.replace_similarity_candidates(
        [
            FolderSimilarityCandidate(
                folder_a=tmp_path / "project",
                folder_b=tmp_path / "project_backup",
                folder_a_id=repo.get_folder_id(tmp_path / "project"),
                folder_b_id=repo.get_folder_id(tmp_path / "project_backup"),
                score=0.95,
                shared_duplicate_files=2,
                shared_signatures=2,
                name_bonus=0.2,
                size_ratio=1.0,
                file_count_ratio=1.0,
                reason="shared_duplicate_files=2, shared_signatures=2, name_bonus=0.20",
            ),
            FolderSimilarityCandidate(
                folder_a=tmp_path / "unclear",
                folder_b=tmp_path / "unclear_copy",
                folder_a_id=repo.get_folder_id(tmp_path / "unclear"),
                folder_b_id=repo.get_folder_id(tmp_path / "unclear_copy"),
                score=0.5,
                shared_duplicate_files=1,
                shared_signatures=1,
                name_bonus=0.0,
                size_ratio=0.8,
                file_count_ratio=0.8,
                reason="mixed evidence",
            ),
        ]
    )
    builder = ProposalBuilder(
        ScanConfig(
            roots=[tmp_path],
            filescan_folder=tmp_path / "filescan",
            database_path=db_path,
            report_path=tmp_path / "report.xlsx",
            similarity_threshold=0.2,
            merge_threshold=0.9,
        )
    )

    proposals = builder.build()

    backup_proposal = next(proposal for proposal in proposals if proposal.action == "mark_backup")
    review_proposal = next(proposal for proposal in proposals if proposal.action == "needs_review")
    assert backup_proposal.target_path == tmp_path / "project"
    assert review_proposal.target_path is None
    assert review_proposal.evidence
    db.close()
