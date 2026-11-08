"""`filescan scan`: the whole inventory in one command.

It first checks the connected drives against config.yml (drive_check.py): it
lists which are attached, asks about new ones in a terminal and saves their
details. A root on the wrong drive is skipped by the scan, so its files are
never marked missing. Then it runs three steps in order, each of which also
exists as a command of its own:

1. scan-pc: record every PC folder and file;
2. import-server: list the server roots with fsagent over SSH;
3. hash-all: give every file without one a full hash, PC and server;

then it writes every report (`filescan report`) and lists what it wrote.

A step with nothing to do is skipped: with no server roots in config.yml there
is no SSH at all, and with no PC roots no PC scan. To scan only some roots,
comment out the others in config.yml. If a step fails (the server is off, for
example), the error is reported and the remaining steps still run; the summary
at the end lists what failed. Every step is restartable, so running scan again
finishes the job.
"""

from __future__ import annotations

import time
import traceback
from pathlib import Path

from filescan.config import load_config
from filescan.dedupe.hash_all import run_hash_all
from filescan.inventory.preflight import check_db_space
from filescan.inventory.scanner import run_scan
from filescan.inventory.server_import import run_import_server
from filescan.inventory.drive_check import check_drives, print_check
from filescan.reporting.report import print_written, run_reports_for_config


def _duration(seconds: float) -> str:
    minutes, seconds = divmod(round(seconds), 60)
    return f"{minutes}m {seconds:02d}s" if minutes else f"{seconds}s"


def run_full_scan(config_path: str | Path, *, rescan: bool = False, delta: bool = False) -> list[str]:
    """Run every inventory step. Returns the names of the steps that failed."""
    print("=== Drives ===")
    failed: list[str] = []
    started = time.monotonic()
    try:
        print_check(check_drives(Path(config_path)))
    except SystemExit:
        raise
    except Exception as exc:  # the scan still checks each root's drive before reading it
        failed.append("drives")
        print(f"The drive check failed: {exc}")
        traceback.print_exc(limit=3)
    timings = [("drives", time.monotonic() - started)]
    config = load_config(config_path)  # read again: the check may have added a drive
    steps = []
    if config.pc_root_configs:
        steps.append(("scan-pc", lambda: (check_db_space(config),
                                          run_scan(config_path, rescan=rescan, delta=delta))))
    if config.server_roots:
        steps.append(("import-server", lambda: run_import_server(config_path)))
    steps.append(("hash-all", lambda: run_hash_all(config_path)))

    for number, (name, step) in enumerate(steps, 1):
        print(f"\n=== Step {number} of {len(steps)}: {name} ===")
        step_started = time.monotonic()
        try:
            step()
        except SystemExit:
            raise  # a deliberate stop, such as too little disk space for the database
        except Exception as exc:  # report, then carry on with the other steps
            failed.append(name)
            print(f"{name} failed: {exc}")
            traceback.print_exc(limit=3)
        timings.append((name, time.monotonic() - step_started))
    print("\n=== Reports ===")
    reports_started = time.monotonic()
    notes = [f"The scan's {name} step failed, so these reports leave out what it would have added."
             for name in failed]
    report = run_reports_for_config(config, notes=notes)
    print_written(report)
    timings.append(("reports", time.monotonic() - reports_started))
    print("\nTime taken: " + ", ".join(f"{name} {_duration(seconds)}" for name, seconds in timings)
          + f"; {_duration(time.monotonic() - started)} in all.")

    print()
    if failed:
        print(f"Finished, but these steps failed: {', '.join(failed)}. Fix the cause and run "
              "`filescan scan` again; finished work is kept, so it carries on where it stopped.")
    else:
        print("Inventory complete. `filescan status` shows what to do next.")
    return failed + [f"report: {name}" for name in report.failed]
