from __future__ import annotations

import shutil
from pathlib import Path

from filescan.config import load_config
from filescan.execution.verifier import files_match
from filescan.models import ExecutionResult, ScanConfig
from filescan.planning.artifacts import load_plan_artifact, write_execution_artifact
from filescan.progress import progress_bar


class ExecutionRunner:
    def __init__(self, config: ScanConfig) -> None:
        self.config = config

    def _iter_source_files(self, source_root: Path) -> list[Path]:
        files: list[Path] = []
        queue: list[Path] = [source_root]
        while queue:
            current = queue.pop()
            try:
                entries = list(current.iterdir())
            except OSError as exc:
                print(f"Skipping folder during execution: {current} ({exc})")
                continue
            for entry in entries:
                try:
                    if entry.is_dir():
                        queue.append(entry)
                    elif entry.is_file():
                        files.append(entry)
                except OSError as exc:
                    print(f"Skipping path during execution: {entry} ({exc})")
        return files

    def _copy_tree(self, source_root: Path, target_root: Path) -> tuple[int, int, list[str]]:
        source_files = self._iter_source_files(source_root)
        copied = 0
        copied_bytes = 0
        errors: list[str] = []
        with progress_bar(desc=f"execute {source_root.name}", total=len(source_files), unit="file") as bar:
            for source_file in source_files:
                relative_path = source_file.relative_to(source_root)
                destination = target_root / relative_path
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists():
                    if files_match(source_file, destination):
                        bar.update(1)
                        continue
                    errors.append(f"Destination already exists with different content: {destination}")
                    bar.update(1)
                    continue
                try:
                    shutil.copy2(source_file, destination)
                    copied += 1
                    copied_bytes += source_file.stat().st_size
                except OSError as exc:
                    errors.append(f"Failed to copy {source_file} to {destination}: {exc}")
                bar.update(1)
        return copied, copied_bytes, errors

    def _verify_tree(self, source_root: Path, target_root: Path) -> bool:
        for source_file in self._iter_source_files(source_root):
            destination = target_root / source_file.relative_to(source_root)
            if not files_match(source_file, destination):
                return False
        return True

    def run(self, plan_path: Path) -> Path:
        plan = load_plan_artifact(plan_path)
        results: list[ExecutionResult] = []
        for proposal in plan["proposals"]:
            if proposal.get("approval_state", "pending") != "approved":
                continue
            action = proposal["action"]
            proposal_id = proposal["proposal_id"]
            target = Path(proposal["target_path"]) if proposal.get("target_path") else None
            source_paths = [Path(item) for item in proposal.get("source_paths", [])]
            if action not in {"merge_folder", "move_folder"} or target is None:
                results.append(
                    ExecutionResult(
                        proposal_id=proposal_id,
                        status="skipped",
                        files_copied=0,
                        bytes_copied=0,
                        verification_passed=action == "mark_backup",
                        source_cleanup_allowed=False,
                        errors=(),
                    )
                )
                continue
            copied_files = 0
            copied_bytes = 0
            errors: list[str] = []
            verification_passed = True
            for source_root in source_paths:
                files_written, bytes_written, source_errors = self._copy_tree(source_root, target)
                copied_files += files_written
                copied_bytes += bytes_written
                errors.extend(source_errors)
                verification_passed = verification_passed and self._verify_tree(source_root, target)
            results.append(
                ExecutionResult(
                    proposal_id=proposal_id,
                    status="completed" if verification_passed and not errors else "failed",
                    files_copied=copied_files,
                    bytes_copied=copied_bytes,
                    verification_passed=verification_passed,
                    source_cleanup_allowed=verification_passed and not errors,
                    errors=tuple(errors),
                )
            )
        return write_execution_artifact(self.config.artifact_dir, plan_id=str(plan["plan_id"]), results=results)


def run_execution(config_path: str | Path, plan_path: str | Path) -> Path:
    return ExecutionRunner(load_config(config_path)).run(Path(plan_path))
