from __future__ import annotations

import argparse
from pathlib import Path

from filescan.config import load_config
from filescan.dedupe.duplicates import run_duplicates
from filescan.execution.mover import run_execution
from filescan.inventory.scanner import run_scan
from filescan.planning.artifacts import load_plan_artifact
from filescan.planning.proposals import build_plan_artifact
from filescan.reporting.xlsx import write_report
from filescan.similarity.folders import run_similarity
from filescan.storage.db import validate_database_ready


def _stage_start(name: str) -> None:
    print(f"Current process: {name}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="filescan")
    parser.add_argument("--config", type=Path, default=Path("config.yml"))
    parser.add_argument("--rescan", action="store_true", help="Re-run scan and analysis stages instead of reusing existing results.")
    subparsers = parser.add_subparsers(dest="command", required=False)

    run_parser = subparsers.add_parser("run", help="Run scan, duplicates, similarity, plan, and report in sequence.")
    run_parser.add_argument("--rescan", action="store_true", help="Re-run scan and analysis stages instead of reusing existing results.")
    run_parser.add_argument("--replan", action="store_true", help="Recreate the plan artifact even if the current one can be reused.")

    scan_parser = subparsers.add_parser("scan")
    scan_parser.add_argument("--rescan", action="store_true", help="Re-scan roots even if they were already scanned.")

    duplicates_parser = subparsers.add_parser("duplicates")
    duplicates_parser.add_argument("--rescan", action="store_true", help="Rebuild duplicate analysis even if cached results exist.")

    similarity_parser = subparsers.add_parser("similarity")
    similarity_parser.add_argument("--rescan", action="store_true", help="Rebuild folder similarity analysis even if cached results exist.")

    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--replan", action="store_true", help="Recreate the plan artifact even if the current one can be reused.")

    execute_parser = subparsers.add_parser("execute")
    execute_parser.add_argument("--plan", type=Path, required=True)

    subparsers.add_parser("report")
    return parser


def _validate_database(config_path: Path) -> None:
    config = load_config(config_path)
    try:
        validate_database_ready(config.database_path)
    except Exception as exc:
        raise SystemExit(f"Database preflight failed: {exc}") from exc
    for output_dir in {config.filescan_folder, config.report_path.parent}:
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            probe_path = output_dir / ".filescan-write-probe"
            probe_path.write_text("ok")
            probe_path.unlink(missing_ok=True)
        except OSError as exc:
            raise SystemExit(f"Output preflight failed: {exc} ({output_dir})") from exc


def _run_pipeline(config_path: Path, *, rescan: bool = False, replan: bool = False) -> dict[str, object]:
    _stage_start("scan")
    scan_run_id = run_scan(config_path, rescan=rescan)
    _stage_start("duplicates")
    duplicate_groups = run_duplicates(config_path, rescan=rescan)
    _stage_start("similarity")
    similarity_candidates = run_similarity(config_path, rescan=rescan)
    _stage_start("plan")
    artifact_path = build_plan_artifact(config_path, replan=replan)
    config = load_config(config_path)
    proposal_count = len(load_plan_artifact(artifact_path).get("proposals", []))
    _stage_start("report")
    report_path = write_report(config_path)
    return {
        "scan_run_id": scan_run_id,
        "duplicate_group_count": len(duplicate_groups),
        "similarity_candidate_count": len(similarity_candidates),
        "proposal_count": proposal_count,
        "plan_artifact": artifact_path,
        "report_path": report_path,
    }


def _print_run_summary(*, config: Path, resolved_config, results: dict[str, object]) -> None:
    print("Run summary:")
    print(f"  config: {config}")
    print(f"  scan: completed (scan_run_id={results['scan_run_id']})")
    print(f"  duplicates: completed ({results['duplicate_group_count']} duplicate groups)")
    print(f"  similarity: completed ({results['similarity_candidate_count']} candidates)")
    print(f"  plan: completed ({results['proposal_count']} proposals)")
    print("  report: completed")
    print(f"  filescan folder: {resolved_config.filescan_folder}")
    print(f"  database: {resolved_config.database_path}")
    print(f"  plan artifact: {results['plan_artifact']}")
    print(f"  report: {results['report_path']}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        args.command = "run"
    _validate_database(args.config)
    config = load_config(args.config)

    if args.command == "run":
        results = _run_pipeline(args.config, rescan=args.rescan, replan=getattr(args, "replan", False))
        _print_run_summary(config=args.config, resolved_config=config, results=results)
    elif args.command == "scan":
        _stage_start("scan")
        run_scan(args.config, rescan=args.rescan)
    elif args.command == "duplicates":
        _stage_start("duplicates")
        run_duplicates(args.config, rescan=args.rescan)
    elif args.command == "similarity":
        _stage_start("similarity")
        run_similarity(args.config, rescan=args.rescan)
    elif args.command == "plan":
        _stage_start("plan")
        artifact_path = build_plan_artifact(args.config, replan=args.replan)
        print(artifact_path)
    elif args.command == "execute":
        _stage_start("execute")
        artifact_path = run_execution(args.config, args.plan)
        print(artifact_path)
    elif args.command == "report":
        _stage_start("report")
        report_path = write_report(args.config)
        print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
