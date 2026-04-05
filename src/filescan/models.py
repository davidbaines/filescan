from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

PlanAction = Literal["keep", "mark_backup", "merge_folder", "move_folder", "needs_review"]
ApprovalState = Literal["pending", "approved", "rejected"]


@dataclass(slots=True)
class ScanConfig:
    roots: list[Path]
    filescan_folder: Path
    database_path: Path
    report_path: Path
    exclude_folders: frozenset[str] = field(default_factory=frozenset)
    exclude_extensions: frozenset[str] = field(default_factory=frozenset)
    min_file_size: int = 0
    max_file_size: int | None = None
    duplicate_size_threshold: int = 1_000_000_000
    similarity_threshold: float = 0.8
    merge_threshold: float = 0.93
    worker_count: int = 4
    backup_name_tokens: tuple[str, ...] = ("backup", "copy", "old", "temp", "temporary")

    @property
    def artifact_dir(self) -> Path:
        return self.filescan_folder


@dataclass(slots=True)
class FolderRecord:
    path: Path
    drive: str
    parent_path: Path | None
    depth: int
    file_count: int
    total_bytes: int
    mtime: float | None
    id: int | None = None
    parent_id: int | None = None
    scan_run_id: int = 0
    is_missing: bool = False


@dataclass(slots=True)
class FileRecord:
    path: Path
    folder_path: Path
    filename: str
    size: int
    mtime: float
    ctime: float
    id: int | None = None
    folder_id: int | None = None
    scan_run_id: int = 0
    is_missing: bool = False
    quick_hash: str | None = None
    full_hash: str | None = None


@dataclass(slots=True)
class DuplicateGroup:
    full_hash: str
    size_bytes: int
    files: tuple[FileRecord, ...]
    id: int | None = None

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def total_bytes(self) -> int:
        return self.file_count * self.size_bytes


@dataclass(slots=True)
class FolderSimilarityCandidate:
    folder_a: Path
    folder_b: Path
    score: float
    shared_duplicate_files: int
    shared_signatures: int
    name_bonus: float
    size_ratio: float
    file_count_ratio: float
    reason: str
    id: int | None = None
    folder_a_id: int | None = None
    folder_b_id: int | None = None


@dataclass(slots=True)
class PlanProposal:
    proposal_id: str
    action: PlanAction
    source_paths: tuple[Path, ...]
    target_path: Path | None
    evidence: tuple[str, ...]
    reason: str
    approval_state: ApprovalState = "pending"


@dataclass(slots=True)
class ExecutionResult:
    proposal_id: str
    status: str
    files_copied: int
    bytes_copied: int
    verification_passed: bool
    source_cleanup_allowed: bool
    errors: tuple[str, ...] = ()
