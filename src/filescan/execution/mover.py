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

    def _execute_cluster(self, cluster: dict) -> ExecutionResult:
        cluster_id = cluster["cluster_id"]
        master_member = next((m for m in cluster["members"] if m["is_master"]), None)
        if master_member is None:
            return ExecutionResult(
                proposal_id=cluster_id, status="failed",
                files_copied=0, bytes_copied=0,
                verification_passed=False, source_cleanup_allowed=False,
                errors=("No master member found in cluster.",),
            )

        master_path = Path(master_member["path"])
        copies = [m for m in cluster["members"] if not m["is_master"]]
        total_copied = 0
        total_bytes = 0
        all_errors: list[str] = []
        all_verified = True

        for copy_member in copies:
            copy_path = Path(copy_member["path"])
            unique_files = [Path(p) for p in copy_member["unique_files"]]

            if not unique_files:
                # All files already present in master — just mark it
                self._write_merged_marker(copy_path, master_path)
                print(f"  {copy_path}: no unique files — already fully represented in master.")
                continue

            copy_ok = True
            with progress_bar(desc=f"merge {copy_path.name}", total=len(unique_files), unit="file") as bar:
                for source_file in unique_files:
                    try:
                        relative = source_file.relative_to(copy_path)
                    except ValueError:
                        relative = Path(source_file.name)
                    destination = master_path / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)

                    if destination.exists():
                        if files_match(source_file, destination):
                            bar.update(1)
                            continue
                        all_errors.append(f"Conflict: {destination} exists with different content")
                        copy_ok = False
                        all_verified = False
                        bar.update(1)
                        continue

                    try:
                        shutil.copy2(source_file, destination)
                        if files_match(source_file, destination):
                            total_copied += 1
                            total_bytes += destination.stat().st_size
                        else:
                            all_errors.append(f"Verification failed after copy: {destination}")
                            copy_ok = False
                            all_verified = False
                    except OSError as exc:
                        all_errors.append(f"Failed to copy {source_file}: {exc}")
                        copy_ok = False
                        all_verified = False
                    bar.update(1)

            if copy_ok:
                self._write_merged_marker(copy_path, master_path)
                print(f"  {copy_path} → fully merged into {master_path}. Safe to delete.")
            else:
                print(f"  {copy_path}: merge incomplete — see errors above.")

        return ExecutionResult(
            proposal_id=cluster_id,
            status="completed" if all_verified and not all_errors else "failed",
            files_copied=total_copied,
            bytes_copied=total_bytes,
            verification_passed=all_verified,
            source_cleanup_allowed=all_verified and not all_errors,
            errors=tuple(all_errors),
        )

    def _write_merged_marker(self, copy_path: Path, master_path: Path) -> None:
        try:
            (copy_path / ".filescan-merged").write_text(
                f"Merged into: {master_path}\n", encoding="utf-8"
            )
        except OSError as exc:
            print(f"  Warning: could not write .filescan-merged marker in {copy_path}: {exc}")

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
                results.append(ExecutionResult(
                    proposal_id=proposal_id, status="skipped",
                    files_copied=0, bytes_copied=0,
                    verification_passed=action == "mark_backup",
                    source_cleanup_allowed=False, errors=(),
                ))
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
            results.append(ExecutionResult(
                proposal_id=proposal_id,
                status="completed" if verification_passed and not errors else "failed",
                files_copied=copied_files, bytes_copied=copied_bytes,
                verification_passed=verification_passed,
                source_cleanup_allowed=verification_passed and not errors,
                errors=tuple(errors),
            ))

        for cluster in plan.get("clusters", []):
            if cluster.get("status") != "approved":
                continue
            results.append(self._execute_cluster(cluster))

        return write_execution_artifact(
            self.config.artifact_dir, plan_id=str(plan["plan_id"]), results=results
        )


def run_execution(config_path: str | Path, plan_path: str | Path) -> Path:
    return ExecutionRunner(load_config(config_path)).run(Path(plan_path))
