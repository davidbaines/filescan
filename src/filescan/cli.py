from __future__ import annotations

import argparse
from pathlib import Path

from filescan.cleanup.delete_folder import run_delete_folder
from filescan.cleanup.plan import run_cleanup_plan
from filescan.cleanup.quarantine import (run_cleanup, run_cleanup_purge,
                                         run_cleanup_restore)
from filescan.cleanup.unwanted import run_unwanted_report
from filescan.cleanup.waste_detector import (find_waste_candidates,
                                             print_waste_report,
                                             write_waste_shortcuts)
from filescan.compare import run_compare, run_hash_server
from filescan.config import ConfigNotSet, load_config, resolve_config_path
from filescan.dedupe.duplicates import run_duplicates
from filescan.execution.mover import run_execution
from filescan.inventory.preflight import check_db_space, check_stale_roots
from filescan.inventory.scanner import run_scan
from filescan.inventory.server_import import run_import_server
from filescan.inventory.volumes import run_drives
from filescan.planning.artifacts import load_plan_artifact
from filescan.planning.proposals import build_plan_artifact
from filescan.reporting.exclusions import run_exclusions_report
from filescan.reporting.largest_files import (_fmt_size, find_large_files,
                                              run_largest)
from filescan.planning.build_plan import run_build_plan
from filescan.reporting.measure import run_measure
from filescan.reporting.xlsx import write_report
from filescan.review.app import run_review
from filescan.similarity.clusters import dump_clusters_json, find_clusters
from filescan.similarity.folders import run_similarity
from filescan.similarity.merge_review import run_merge_review
from filescan.status import run_status
from filescan.storage.db import validate_database_ready


def _stage_start(name: str) -> None:
    print(f"Current process: {name}")


# The help lists commands in these groups, in the order they are normally used.
# Each entry is (name, one-line help). Commands of the planned phases are listed
# in PLANNED_HELP only, because they do not exist yet.
COMMAND_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("Start here", [
        ("status", "Show where the work stands and what to run next. Reads only."),
        ("drives", "Show each PC root's drive, its volume serial number and whether the right drive is connected."),
    ]),
    ("1. Inventory: record and hash every file", [
        ("scan", "Walk the PC roots and record every folder and file (size, dates) in the database."),
        ("import-server", "List the server roots with fsagent over SSH and load them into the database."),
        ("duplicates", "Hash files that share a size and record groups of identical files."),
        ("hash-server", "Hash, on the server, every server file whose size occurs more than once (resumable)."),
    ]),
    ("2. Find what can go", [
        ("exclusions-report", "List the folders each exclusion rule leaves out of the organised set, with sizes (CSV)."),
        ("unwanted-report", "Apply the unwanted-data rules outside the exclusions and report matches by folder (CSV)."),
        ("compare", "List PC files with no identical copy on the server (hashes only files whose size matches)."),
        ("review", "Review a delete-candidate exclusion (such as server-trash) folder by folder: keep or trash."),
        ("measure", "Distinct size of the organised set, against PC and server disk space."),
    ]),
    ("3. Clean up", [
        ("delete-folder", "Report on whole PC folders, then delete them with --yes and update the database."),
        ("cleanup-plan", "List every file marked trash in a review, or matched by a delete-candidate rule (CSV)."),
        ("cleanup", "Move the files in a reviewed cleanup plan to quarantine on the same disk (reversible)."),
        ("cleanup-restore", "Put quarantined files from a plan back where they were."),
        ("cleanup-purge", "Delete a plan's quarantined files for good, after a waiting period. Needs --yes."),
    ]),
    ("4. Build the master on disk2", [
        ("build-plan", "Decide where each kept file goes in the organised layout (layout.yml). Copies nothing."),
    ]),
    ("Older merge workflow and debugging (not part of the steps above)", [
        ("run", "Run scan, duplicates, similarity, plan and report in sequence."),
        ("similarity", "Find pairs of folders with similar contents (candidates for merging)."),
        ("plan", "Turn duplicates and similar folders into merge proposals (a plan file, for review)."),
        ("merge-review", "Interactive TUI to review and approve folder merge clusters."),
        ("execute", "Carry out the approved proposals in a merge plan (copies, verified by hash)."),
        ("report", "Write the Excel report of duplicates, similar folders and proposals."),
        ("waste", "Find junk and cache folders by name and report them."),
        ("largest", "List the largest files in the inventory."),
        ("clusters", "Debug: dump folder clusters as JSON."),
    ]),
]

PLANNED_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("4. Build the master on disk2, continued (planned, not built yet)", [
        ("copy-master", "Run a build plan's copies to disk2, verified by hash, keeping each file's date."),
        ("verify-coverage", "Prove every kept file is in the master, by hash."),
    ]),
    ("5. Organise the PC and mirror it (planned, not built yet)", [
        ("pc-plan", "Lay out F:, E: and D: to match the master: move on the same drive, copy the rest."),
    ]),
]

CONFIG_HELP = """\
The configuration file comes from --config, or else from FILESCAN_CONFIG, set in
the environment or in the .env file at the root of the repository:
  FILESCAN_CONFIG=C:\\filescan\\v2\\config.yml

Run "filescan <command> -h" for a command's options."""

HELP: dict[str, str] = {name: text for _, items in COMMAND_GROUPS for name, text in items}


def _commands_text() -> str:
    width = max(len(name) for name in HELP) + 2
    groups = COMMAND_GROUPS[:-1] + PLANNED_GROUPS + COMMAND_GROUPS[-1:]
    blocks = ["\n".join([title] + [f"  {name:<{width}}{text}" for name, text in items]) for title, items in groups]
    return "\n\n".join(blocks)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="filescan",
        usage="filescan [--config CONFIG] <command> [options]",
        description="Inventory, compare and tidy files on the PC and the server. Files are deleted only by "
        "cleanup-purge --yes and delete-folder --yes.\n\n" + _commands_text(),
        epilog=CONFIG_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", type=Path, default=None,
                        help="Configuration file. Defaults to FILESCAN_CONFIG (environment or .env).")
    parser.add_argument("--rescan", action="store_true", help="Re-run scan and analysis stages instead of reusing existing results.")
    # The grouped list above replaces argparse's own list of commands.
    subparsers = parser.add_subparsers(dest="command", required=False, metavar="<command>", help=argparse.SUPPRESS)

    def add(name: str) -> argparse.ArgumentParser:
        return subparsers.add_parser(name, help=HELP[name], description=HELP[name], prog=f"filescan {name}")

    run_parser = add("run")
    run_parser.add_argument("--rescan", action="store_true", help="Re-run scan and analysis stages instead of reusing existing results.")
    run_parser.add_argument("--replan", action="store_true", help="Recreate the plan artifact even if the current one can be reused.")

    add("status")

    scan_parser = add("scan")
    scan_parser.add_argument("--rescan", action="store_true", help="Re-scan roots even if they were already scanned.")
    scan_parser.add_argument("--delta", action="store_true", help="Fast update: skip folders whose mtime is unchanged since last scan. Misses files edited in place; use --rescan before comparing or hashing.")

    duplicates_parser = add("duplicates")
    duplicates_parser.add_argument(
        "--rescan", action="store_true",
        help="Run even if no scan has happened since last time. Existing hashes are kept; "
        "only files without one are hashed.")

    similarity_parser = add("similarity")
    similarity_parser.add_argument("--rescan", action="store_true", help="Rebuild folder similarity analysis even if cached results exist.")

    plan_parser = add("plan")
    plan_parser.add_argument("--replan", action="store_true", help="Recreate the plan artifact even if the current one can be reused.")

    execute_parser = add("execute")
    execute_parser.add_argument("--plan", type=Path, required=True, help="The approved plan file.")

    for name in ("report", "waste", "largest", "import-server", "compare", "hash-server", "unwanted-report",
                 "measure", "drives", "exclusions-report", "build-plan"):
        add(name)
    review_parser = add("review")
    review_parser.add_argument("scope", nargs="?", default=None, help="Name of a delete-candidate exclusion rule.")
    review_parser.add_argument("--split-gb", type=float, default=5.0,
                               help="Split folders holding more unique data than this (default 5 GB).")
    df = add("delete-folder")
    df.add_argument("folders", nargs="+", help="PC folders to delete, each with everything below it.")
    df.add_argument("--yes", action="store_true", help="Really delete. Without it, only report what would go.")
    df.add_argument("--take-ownership", action="store_true",
                    help="First take ownership and reset permissions (takeown, icacls) on each folder. Needs an "
                    "elevated terminal; never allowed on C:.")
    cp = add("cleanup-plan")
    cp.add_argument("--only", choices=("pc", "server"), help="Plan only PC files or only server files.")
    cp.add_argument("--no-rules", action="store_true", help="Leave out unwanted-data rules; reviews only.")
    cq = add("cleanup")
    cq.add_argument("--plan", required=True, help="Plan folder (or its files.csv) from cleanup-plan.")
    cq.add_argument("--only", choices=("pc", "server"), help="Only PC files or only server files.")
    cq.add_argument("--dry-run", action="store_true", help="Count what would move; move nothing.")
    cq.add_argument("--reason", help="Only this reason from the plan, e.g. review:server-trash, or a prefix such as rule:")
    cr = add("cleanup-restore")
    cr.add_argument("--plan", required=True, help="Plan folder (or its files.csv) from cleanup-plan.")
    cr.add_argument("--only", choices=("pc", "server"), help="Only PC files or only server files.")
    cr.add_argument("--reason", help="Only this reason from the plan, e.g. review:app-caches, or a prefix such as rule:")
    cx = add("cleanup-purge")
    cx.add_argument("--plan", required=True, help="Plan folder (or its files.csv) from cleanup-plan.")
    cx.add_argument("--only", choices=("pc", "server"), help="Only PC files or only server files.")
    cx.add_argument("--older-than-days", type=float, default=14.0,
                    help="Only files quarantined at least this long ago (default 14).")
    cx.add_argument("--yes", action="store_true", help="Really delete. Without it, only report what would go.")
    cx.add_argument("--reason", help="Only this reason from the plan, or a prefix such as rule:")
    clusters_parser = add("clusters")
    clusters_parser.add_argument("--show-suppressed", action="store_true", help="Include hierarchy-suppressed clusters.")

    merge_review_parser = add("merge-review")
    merge_review_parser.add_argument("--plan", type=Path, default=None, help="Path to plan artifact JSON (default: latest in filescan folder).")
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


def _check_scan_space(config_path: Path) -> None:
    check_db_space(load_config(config_path))


def _run_pipeline(config_path: Path, *, rescan: bool = False, replan: bool = False) -> dict[str, object]:
    _check_scan_space(config_path)
    _stage_start("scan")
    scan_run_id = run_scan(config_path, rescan=rescan)
    _stage_start("duplicates")
    duplicate_groups = run_duplicates(config_path, rescan=rescan)
    _stage_start("similarity")
    similarity_candidates = run_similarity(config_path, rescan=rescan)
    _stage_start("plan")
    artifact_path = build_plan_artifact(config_path, replan=replan)
    config = load_config(config_path)
    _plan = load_plan_artifact(artifact_path)
    proposal_count = len(_plan.get("proposals", []))
    cluster_count = len(_plan.get("clusters", []))
    _stage_start("report")
    report_path = write_report(config_path)
    _stage_start("waste")
    waste_candidates = find_waste_candidates(config_path)
    print_waste_report(waste_candidates)
    waste_shortcut_dir = None
    if waste_candidates:
        waste_shortcut_dir = write_waste_shortcuts(waste_candidates, config.filescan_folder / "waste_review")
    _stage_start("largest")
    large_files = find_large_files(config)
    if large_files:
        total_bytes = sum(size for _, size in large_files)
        print(f"  {len(large_files)} files found ({_fmt_size(total_bytes)} total).")
        print("  Run 'filescan largest' to review interactively.")
    else:
        print(f"  No files found above the threshold ({_fmt_size(config.large_file_size)}).")
    return {
        "scan_run_id": scan_run_id,
        "duplicate_group_count": len(duplicate_groups),
        "similarity_candidate_count": len(similarity_candidates),
        "proposal_count": proposal_count,
        "cluster_count": cluster_count,
        "plan_artifact": artifact_path,
        "report_path": report_path,
        "waste_candidate_count": len(waste_candidates),
        "waste_shortcut_dir": waste_shortcut_dir,
        "large_file_count": len(large_files),
    }


def _print_run_summary(*, config: Path, resolved_config, results: dict[str, object]) -> None:
    print("Run summary:")
    print(f"  config: {config}")
    print(f"  scan: completed (scan_run_id={results['scan_run_id']})")
    print(f"  duplicates: completed ({results['duplicate_group_count']} duplicate groups)")
    print(f"  similarity: completed ({results['similarity_candidate_count']} candidates)")
    print(f"  plan: completed ({results['proposal_count']} proposals, {results['cluster_count']} clusters)")
    print("  report: completed")
    print(f"  waste: {results['waste_candidate_count']} candidates found")
    print(f"  largest: {results['large_file_count']} large files found")
    print(f"  filescan folder: {resolved_config.filescan_folder}")
    print(f"  database: {resolved_config.database_path}")
    print(f"  plan artifact: {results['plan_artifact']}")
    print(f"  report: {results['report_path']}")
    if results["waste_shortcut_dir"]:
        print(f"  waste shortcuts: {results['waste_shortcut_dir']}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0
    try:
        args.config, args.config_source = resolve_config_path(args.config)
    except ConfigNotSet as exc:
        raise SystemExit(str(exc)) from exc
    _validate_database(args.config)
    config = load_config(args.config)

    if args.command not in ("scan", "import-server", "status", "drives"):
        check_stale_roots(config)

    if args.command == "run":
        results = _run_pipeline(args.config, rescan=args.rescan, replan=getattr(args, "replan", False))
        _print_run_summary(config=args.config, resolved_config=config, results=results)
    elif args.command == "scan":
        _check_scan_space(args.config)
        _stage_start("scan")
        run_scan(args.config, rescan=args.rescan, delta=getattr(args, "delta", False))
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
    elif args.command == "waste":
        _stage_start("waste")
        candidates = find_waste_candidates(args.config)
        print_waste_report(candidates)
        if candidates:
            shortcut_dir = write_waste_shortcuts(candidates, config.filescan_folder / "waste_review")
            print(f"\nShortcut folder: {shortcut_dir}")
            print("Open that folder in Explorer — double-click any shortcut to browse the candidate.")
    elif args.command == "largest":
        _stage_start("largest")
        run_largest(args.config)
    elif args.command == "import-server":
        _stage_start("import-server")
        run_import_server(args.config)
    elif args.command == "compare":
        _stage_start("compare")
        run_compare(args.config)
    elif args.command == "hash-server":
        _stage_start("hash-server")
        run_hash_server(args.config)
    elif args.command == "unwanted-report":
        _stage_start("unwanted-report")
        run_unwanted_report(args.config)
    elif args.command == "measure":
        _stage_start("measure")
        run_measure(args.config)
    elif args.command == "drives":
        run_drives(config)
    elif args.command == "status":
        run_status(config, config_path=args.config, config_source=args.config_source)
    elif args.command == "delete-folder":
        _stage_start("delete-folder")
        run_delete_folder(config, args.folders, yes=args.yes, take_ownership=args.take_ownership)
    elif args.command == "review":
        _stage_start("review")
        run_review(args.config, args.scope, split_gb=args.split_gb)
    elif args.command == "build-plan":
        _stage_start("build-plan")
        run_build_plan(args.config)
    elif args.command == "cleanup-plan":
        _stage_start("cleanup-plan")
        run_cleanup_plan(args.config, only=args.only, include_rules=not args.no_rules)
    elif args.command == "cleanup":
        _stage_start("cleanup")
        run_cleanup(args.config, args.plan, only=args.only, dry_run=args.dry_run, reason=args.reason)
    elif args.command == "cleanup-restore":
        _stage_start("cleanup-restore")
        run_cleanup_restore(args.config, args.plan, only=args.only, reason=args.reason)
    elif args.command == "cleanup-purge":
        _stage_start("cleanup-purge")
        run_cleanup_purge(args.config, args.plan, older_than_days=args.older_than_days, yes=args.yes, only=args.only,
                          reason=args.reason)
    elif args.command == "exclusions-report":
        _stage_start("exclusions-report")
        print(run_exclusions_report(args.config))
    elif args.command == "clusters":
        clusters = find_clusters(args.config)
        if not getattr(args, "show_suppressed", False):
            clusters = [c for c in clusters if not c.is_suppressed]
        print(dump_clusters_json(clusters))
    elif args.command == "merge-review":
        run_merge_review(args.config, plan_path=getattr(args, "plan", None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
