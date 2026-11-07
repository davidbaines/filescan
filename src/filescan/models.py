from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Literal

PlanAction = Literal["keep", "mark_backup", "merge_folder", "move_folder", "needs_review"]
ApprovalState = Literal["pending", "approved", "rejected"]
RootLocation = Literal["pc", "server"]
RootRole = Literal["source", "master"]

# Folder at the top of each root that holds quarantined files (filescan cleanup).
# The scanner and the server listing always skip it, so quarantined files are
# never indexed again or counted as copies.
QUARANTINE_DIR = "_filescan_quarantine"


@dataclass(frozen=True, slots=True)
class RootConfig:
    """One configured root.

    location: "pc" roots are walked by the scanner; "server" roots are listed by
    the server helper and never walked or hashed over the network.
    role: "source" roots feed the organised set; "master" is where it is built.
    skip: absolute folders never inventoried (data that never leaves the PC).
    """

    path: Path
    location: RootLocation = "pc"
    role: RootRole = "source"
    skip: tuple[Path, ...] = ()
    server_path: str | None = None  # POSIX mount point of a server root
    exclude_dirs: tuple[str, ...] = ()  # folder names fsagent leaves out, e.g. lost+found
    name: str = ""  # friendly name, e.g. LaCie Quadra
    serial: str = ""  # volume serial number as `vol` shows it, e.g. A1B2-C3D4; checked before scanning
    volume_id: str = ""  # from the drive's marker file (`filescan drives --add`); see inventory/volumes.py


@dataclass(frozen=True, slots=True)
class ExclusionRule:
    """A named pattern for data left out of the organised set.

    Excluded data is still inventoried; rules are applied when querying, so the
    excluded bytes stay visible for review before anything is wiped.
    """

    name: str
    pattern: re.Pattern[str]
    reason: str = ""
    # "delete-candidate": listed for review and possible deletion (old OS, caches).
    # "keep-in-place": left where it is, never deleted (e.g. data hosted elsewhere).
    disposition: str = "delete-candidate"

    def matches(self, folder: Path) -> bool:
        # A trailing separator lets patterns anchor on whole folder names.
        return self.pattern.search(str(folder).rstrip("\\") + "\\") is not None


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
    large_file_size: int = 500_000_000
    large_file_thresholds: dict[Path, int] = field(default_factory=dict)
    scan_max_age: timedelta = timedelta(days=30)
    waste_file_size: int = 0

    similarity_cluster_threshold: float = 0.70
    root_configs: list[RootConfig] = field(default_factory=list)
    exclusions: tuple[ExclusionRule, ...] = ()
    pinned: tuple[Path, ...] = ()
    server_ssh: str = ""  # e.g. david@192.168.1.133
    server_python: str = "python3"

    def large_file_size_for(self, root: Path) -> int:
        return self.large_file_thresholds.get(root, self.large_file_size)

    def _configs(self) -> list[RootConfig]:
        return self.root_configs or [RootConfig(path=root) for root in self.roots]

    @property
    def pc_roots(self) -> list[Path]:
        return [rc.path for rc in self._configs() if rc.location == "pc"]

    @property
    def pc_root_configs(self) -> list[RootConfig]:
        return [rc for rc in self._configs() if rc.location == "pc"]

    @property
    def server_roots(self) -> list[Path]:
        return [rc.path for rc in self._configs() if rc.location == "server"]

    @property
    def skip_paths(self) -> frozenset[Path]:
        return frozenset(path for rc in self._configs() for path in rc.skip)

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
    attributes: int = 0  # Windows st_file_attributes
