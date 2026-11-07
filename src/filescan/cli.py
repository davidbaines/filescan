from __future__ import annotations

import argparse
from pathlib import Path

from filescan.cleanup.delete_folder import run_delete_folder
from filescan.cleanup.plan import run_cleanup_plan
from filescan.cleanup.quarantine import run_cleanup, run_cleanup_purge, run_cleanup_restore
from filescan.config import ConfigNotSet, load_config, resolve_config_path
from filescan.dedupe.hash_all import run_hash_all
from filescan.inventory.full_scan import run_full_scan
from filescan.inventory.preflight import check_db_space, check_stale_roots
from filescan.inventory.scanner import run_scan
from filescan.inventory.server_import import run_import_server
from filescan.inventory.volumes import run_drives
from filescan.planning.build_plan import run_build_plan
from filescan.reporting.report import run_reports
from filescan.review.app import run_review
from filescan.status import run_status
from filescan.storage.db import validate_database_ready

# The help lists commands in these groups, in the order they are normally used.
# README.md describes the same order; a test checks that the two agree.
COMMAND_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("Start here", [
        ("status", "Show where the work stands and what to run next. Reads only."),
    ]),
    ("1. Inventory and reports", [
        ("scan", "Record and hash every file, PC and server, then write the reports to one workbook."),
        ("report", "Write the reports again without scanning (scan writes them when it finishes)."),
    ]),
    ("2. Decide what can go", [
        ("review", "Review a delete-candidate exclusion (such as server-trash) folder by folder: keep or trash."),
    ]),
    ("3. Clean up", [
        ("cleanup-plan", "List every file marked trash in a review, or matched by a delete-candidate rule (CSV)."),
        ("cleanup", "Move the files in a reviewed cleanup plan to quarantine on the same disk (reversible)."),
        ("cleanup-restore", "Put quarantined files from a plan back where they were."),
        ("cleanup-purge", "Delete a plan's quarantined files for good, after a waiting period. Needs --yes."),
        ("delete-folder", "Report on whole PC folders, then delete them with --yes and update the database."),
    ]),
    ("4. Build the master on disk2", [
        ("build-plan", "Decide where each kept file goes in the organised layout (layout.yml). Copies nothing."),
    ]),
    ("The steps of scan, on their own (only when one step is needed)", [
        ("drives", "Show each PC root's drive, its volume serial number and whether the right drive is connected."),
        ("scan-pc", "Walk the PC roots and record every folder and file (size, dates) in the database."),
        ("import-server", "List the server roots with fsagent over SSH and load them into the database."),
        ("hash-all", "Give every file a full hash, PC and server (resumable; reads only what is missing)."),
    ]),
]

# Listed in the help so the whole process is visible, but not commands yet.
PLANNED_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("Planned, not built yet", [
        ("similar-trees", "Folder trees whose files are contained in, or nearly the same as, another tree."),
        ("copy-master", "Run a build plan's copies to disk2, verified by hash, keeping each file's date."),
        ("verify-coverage", "Prove every kept file is in the master, by hash."),
        ("pc-plan", "Lay out F:, E: and D: to match the master: move on the same drive, copy the rest."),
    ]),
]

CONFIG_HELP = """\
The configuration file comes from --config, or else from FILESCAN_CONFIG, set in
the environment or in the .env file at the root of the repository:
  FILESCAN_CONFIG=C:\\filescan\\v2\\config.yml

Run "filescan <command> -h" for a command's options. README.md explains each step."""

HELP: dict[str, str] = {name: text for _, items in COMMAND_GROUPS for name, text in items}

# Commands that make or check the inventory, so a warning about an old scan is beside the point.
_NO_STALE_CHECK = ("scan", "scan-pc", "import-server", "hash-all", "status", "drives", "report")


def _commands_text() -> str:
    width = max(len(name) for _, items in COMMAND_GROUPS + PLANNED_GROUPS for name, _ in items) + 2
    blocks = ["\n".join([title] + [f"  {name:<{width}}{text}" for name, text in items])
              for title, items in COMMAND_GROUPS + PLANNED_GROUPS]
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
    # The grouped list above replaces argparse's own list of commands.
    subparsers = parser.add_subparsers(dest="command", required=False, metavar="<command>", help=argparse.SUPPRESS)

    def add(name: str) -> argparse.ArgumentParser:
        return subparsers.add_parser(name, help=HELP[name], description=HELP[name], prog=f"filescan {name}")

    for name in ("status", "drives", "import-server", "hash-all", "report", "build-plan"):
        add(name)

    for name in ("scan", "scan-pc"):
        scan = add(name)
        scan.add_argument("--rescan", action="store_true", help="Walk the PC roots again even if already scanned.")
        scan.add_argument("--delta", action="store_true",
                          help="Fast update: skip PC folders whose date is unchanged since the last scan. Misses "
                          "files edited in place.")

    review = add("review")
    review.add_argument("scope", nargs="?", default=None, help="Name of a delete-candidate exclusion rule.")
    review.add_argument("--split-gb", type=float, default=5.0,
                        help="Split folders holding more unique data than this (default 5 GB).")

    plan = add("cleanup-plan")
    plan.add_argument("--only", choices=("pc", "server"), help="Plan only PC files or only server files.")
    plan.add_argument("--no-rules", action="store_true", help="Leave out unwanted-data rules; reviews only.")
    for name in ("cleanup", "cleanup-restore", "cleanup-purge"):
        sub = add(name)
        sub.add_argument("--plan", required=True, help="Plan folder (or its files.csv) from cleanup-plan.")
        sub.add_argument("--only", choices=("pc", "server"), help="Only PC files or only server files.")
        sub.add_argument("--reason", help="Only this reason from the plan, e.g. review:server-trash, "
                         "or a prefix such as rule:")
        if name == "cleanup":
            sub.add_argument("--dry-run", action="store_true", help="Count what would move; move nothing.")
        if name == "cleanup-purge":
            sub.add_argument("--older-than-days", type=float, default=14.0,
                             help="Only files quarantined at least this long ago (default 14).")
            sub.add_argument("--yes", action="store_true", help="Really delete. Without it, only report what would go.")

    df = add("delete-folder")
    df.add_argument("folders", nargs="+", help="PC folders to delete, each with everything below it.")
    df.add_argument("--yes", action="store_true", help="Really delete. Without it, only report what would go.")
    df.add_argument("--take-ownership", action="store_true",
                    help="First take ownership and reset permissions on each folder, one folder at a time. Needs an "
                    "elevated terminal; never allowed on C:.")
    return parser


def _validate_database(config_path: Path) -> None:
    config = load_config(config_path)
    try:
        validate_database_ready(config.database_path)
    except Exception as exc:
        raise SystemExit(f"Database preflight failed: {exc}") from exc
    try:
        config.filescan_folder.mkdir(parents=True, exist_ok=True)
        probe_path = config.filescan_folder / ".filescan-write-probe"
        probe_path.write_text("ok")
        probe_path.unlink(missing_ok=True)
    except OSError as exc:
        raise SystemExit(f"Output preflight failed: {exc} ({config.filescan_folder})") from exc


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

    if args.command not in _NO_STALE_CHECK:
        check_stale_roots(config)
    if args.command not in ("status", "drives"):
        print(f"Current process: {args.command}")

    command = args.command
    if command == "status":
        run_status(config, config_path=args.config, config_source=args.config_source)
    elif command == "drives":
        run_drives(config)
    elif command == "scan":
        failed = run_full_scan(args.config, rescan=args.rescan, delta=args.delta)
        return 1 if failed else 0
    elif command == "scan-pc":
        check_db_space(config)
        run_scan(args.config, rescan=args.rescan, delta=args.delta)
    elif command == "import-server":
        run_import_server(args.config)
    elif command == "hash-all":
        run_hash_all(args.config)
    elif command == "report":
        return 1 if run_reports(args.config).failed else 0
    elif command == "review":
        run_review(args.config, args.scope, split_gb=args.split_gb)
    elif command == "cleanup-plan":
        run_cleanup_plan(args.config, only=args.only, include_rules=not args.no_rules)
    elif command == "cleanup":
        run_cleanup(args.config, args.plan, only=args.only, dry_run=args.dry_run, reason=args.reason)
    elif command == "cleanup-restore":
        run_cleanup_restore(args.config, args.plan, only=args.only, reason=args.reason)
    elif command == "cleanup-purge":
        run_cleanup_purge(args.config, args.plan, older_than_days=args.older_than_days, yes=args.yes,
                          only=args.only, reason=args.reason)
    elif command == "delete-folder":
        run_delete_folder(config, args.folders, yes=args.yes, take_ownership=args.take_ownership)
    elif command == "build-plan":
        run_build_plan(args.config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
