"""`filescan scan`: the whole inventory in one command.

It runs four steps in order, each of which also exists as a command of its own:

1. drives: check each PC root is on the drive it should be (a root on the
   wrong drive is skipped by the scan, so its files are never marked missing);
2. scan-pc: record every PC folder and file;
3. import-server: list the server roots with fsagent over SSH;
4. hash-all: give every file without one a full hash, PC and server;

then it writes every report (`filescan report`) and lists what it wrote.

A step with nothing to do is skipped: with no server roots in config.yml there
is no SSH at all, and with no PC roots no PC scan. To scan only some roots,
comment out the others in config.yml. If a step fails (the server is off, for
example), the error is reported and the remaining steps still run; the summary
at the end lists what failed. Every step is restartable, so running scan again
finishes the job.
"""

from __future__ import annotations

import traceback
from pathlib import Path

from filescan.config import load_config
from filescan.dedupe.hash_all import run_hash_all
from filescan.inventory.preflight import check_db_space
from filescan.inventory.scanner import run_scan
from filescan.inventory.server_import import run_import_server
from filescan.inventory.volumes import run_drives
from filescan.reporting.report import print_written, run_reports_for_config


def run_full_scan(config_path: str | Path, *, rescan: bool = False, delta: bool = False) -> list[str]:
    """Run every inventory step. Returns the names of the steps that failed."""
    config = load_config(config_path)
    steps = [("drives", lambda: run_drives(config))]
    if config.pc_root_configs:
        steps.append(("scan-pc", lambda: (check_db_space(config),
                                          run_scan(config_path, rescan=rescan, delta=delta))))
    if config.server_roots:
        steps.append(("import-server", lambda: run_import_server(config_path)))
    steps.append(("hash-all", lambda: run_hash_all(config_path)))

    failed: list[str] = []
    for number, (name, step) in enumerate(steps, 1):
        print(f"\n=== Step {number} of {len(steps)}: {name} ===")
        try:
            step()
        except SystemExit:
            raise  # a deliberate stop, such as too little disk space for the database
        except Exception as exc:  # report, then carry on with the other steps
            failed.append(name)
            print(f"{name} failed: {exc}")
            traceback.print_exc(limit=3)
    print("\n=== Reports ===")
    notes = [f"The scan's {name} step failed, so these reports leave out what it would have added."
             for name in failed]
    report = run_reports_for_config(config, notes=notes)
    print_written(report)

    print()
    if failed:
        print(f"Finished, but these steps failed: {', '.join(failed)}. Fix the cause and run "
              "`filescan scan` again; finished work is kept, so it carries on where it stopped.")
    else:
        print("Inventory complete. `filescan status` shows what to do next.")
    return failed + [f"report: {name}" for name in report.failed]
