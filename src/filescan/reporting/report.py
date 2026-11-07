"""`filescan report`: write every report, and gather everything in one workbook.

Two separate jobs:

- run_reports_for_config writes each report as a CSV in the filescan folder.
  `filescan scan` runs it when it finishes, so the reports match the latest
  inventory. It reads the database and writes only reports.
- assemble_workbook builds the workbook from the latest CSVs on disk, whichever
  command wrote them. It reads nothing else, so `build-plan` can add its
  sheets in a few seconds without running the reports again.

The reports:

- Measure: the size of the organised set (each content counted once) against
  the free space on the PC drives and the server disks (measure.csv);
- Exclusions: the folders each exclusion in config.yml leaves out of the
  organised set (exclusions_report.csv);
- Unwanted: matches of the unwanted-data rules, by folder (unwanted_report.csv,
  with every file in unwanted_files.csv). It also stores the matches that
  cleanup-plan and build-plan use;
- Missing from server: PC files with no identical copy on the server, by top
  folder (compare_summary.csv, with every file in compare_missing_files.csv).

The workbook (report_filename in config.yml, filescan_report.xlsx by default)
also holds the latest build plan's summary, unsorted folders, renamed files
and long paths. Lists that can be longer than Excel allows stay as CSV only;
the Contents sheet names them.
"""

from __future__ import annotations

import csv
import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from filescan.cleanup.unwanted import run_unwanted_report_for_config
from filescan.compare import run_compare_for_config
from filescan.config import load_config
from filescan.models import ScanConfig
from filescan.problems import write_unreadable_report
from filescan.reporting.exclusions import write_exclusions_report
from filescan.reporting.measure import run_measure_for_config
from filescan.reporting.xlsx import MAX_ROWS, Rows, write_workbook

NOTES_FILE = "report_notes.json"  # notes from the last report run, kept for later re-assembly

# (sheet, file in the filescan folder, what it holds). "build:" means the latest build plan's folder.
SHEETS = [
    ("Measure", "measure.csv", "Size of the organised set against free disk space"),
    ("Exclusions", "exclusions_report.csv", "Folders each exclusion leaves out of the organised set"),
    ("Unwanted", "unwanted_report.csv", "Unwanted-data rule matches, by folder"),
    ("Missing from server", "compare_summary.csv", "PC files with no identical copy on the server, by top folder"),
    ("Unreadable", "unreadable_by_folder.csv", "Files and folders filescan could not read, by folder"),
    ("Build summary", "build:summary.csv", "The latest build plan: files and bytes per category"),
    ("Unsorted", "build:unsorted.csv", "Source folders not in layout.yml, which go to Unsorted"),
    ("Renamed", "build:renamed.csv", "Files renamed to avoid a name clash in the organised set"),
    ("Long paths", "build:problems.csv", "Destinations too long for the server or near the Windows limit"),
]
CSV_ONLY = [
    ("unwanted_files.csv", "Every file an unwanted-data rule matches"),
    ("compare_missing_files.csv", "Every PC file with no identical copy on the server"),
    ("unreadable_files.csv", "Every file and folder filescan could not read"),
    ("build:dropped.csv", "Every copy the build plan leaves out, and where its content is kept"),
    ("build:unhashed.csv", "Kept files with no full hash yet"),
    ("build:manifest.csv", "Every file in the organised set and where it is copied from"),
]


@dataclass
class ReportResult:
    workbook: Path | None = None
    written: list[str] = field(default_factory=list)  # reports written by this run
    failed: list[str] = field(default_factory=list)
    sheets: list[tuple[str, Path, str]] = field(default_factory=list)  # (sheet, source, what it holds)
    csv_only: list[tuple[Path, str]] = field(default_factory=list)


def latest_build(config: ScanConfig) -> Path | None:
    plans = sorted((config.filescan_folder / "build").glob("*/plan.json"))
    return plans[-1].parent if plans else None


def _source(config: ScanConfig, name: str, build: Path | None) -> Path | None:
    if name.startswith("build:"):
        return build / name[len("build:"):] if build else None
    return config.filescan_folder / name


def _read_csv(path: Path) -> Rows:
    rows: Rows = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.reader(handle):
            rows.append(row)
            if len(rows) > MAX_ROWS:
                break
    return rows


def _when(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")


def assemble_workbook(config: ScanConfig) -> ReportResult:
    """Build the workbook from the latest CSVs on disk. A report not yet written is left out."""
    result = ReportResult()
    build = latest_build(config)
    for sheet, name, about in SHEETS:
        path = _source(config, name, build)
        if path is not None and path.exists():
            result.sheets.append((sheet, path, about))
    for name, about in CSV_ONLY:
        path = _source(config, name, build)
        if path is not None and path.exists():
            result.csv_only.append((path, about))

    contents: Rows = [["sheet or file", "what it holds", "written", "source"]]
    contents += [[sheet, about, _when(path), str(path)] for sheet, path, about in result.sheets]
    contents += [["(CSV only)", about, _when(path), str(path)] for path, about in result.csv_only]
    contents.append([])
    contents.append([f"Workbook assembled {datetime.now():%Y-%m-%d %H:%M}"])
    try:
        notes = json.loads((config.filescan_folder / NOTES_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        notes = {}
    contents += [[note] for note in notes.get("notes", [])]
    measure = config.filescan_folder / "measure.csv"
    if build is not None and measure.exists() and (build / "plan.json").stat().st_mtime < measure.stat().st_mtime:
        contents.append(["The build plan sheets come from a plan made before the latest reports; "
                         "run build-plan again for an up-to-date plan."])

    sheets = [("Contents", contents)] + [(sheet, _read_csv(path)) for sheet, path, _ in result.sheets]
    try:
        result.workbook = write_workbook(config.report_path, sheets)
    except PermissionError:
        # Excel keeps an open workbook locked; write a copy beside it instead.
        alternative = config.report_path.with_name(f"{config.report_path.stem}-{datetime.now():%Y%m%d-%H%M%S}.xlsx")
        result.workbook = write_workbook(alternative, sheets)
        print(f"{config.report_path} is open in another program, so the workbook went to {alternative.name}.")
    return result


def run_reports_for_config(config: ScanConfig, *, notes: list[str] | None = None) -> ReportResult:
    """Write each report, then the workbook. A report that fails is listed, and the others are still written."""
    written: list[str] = []
    failed: list[str] = []
    steps = [("unreadable", lambda: write_unreadable_report(config)),
             ("unwanted", lambda: run_unwanted_report_for_config(config)),
             ("measure", lambda: run_measure_for_config(config)),
             ("exclusions", lambda: write_exclusions_report(config))]
    if config.server_roots:
        steps.append(("missing from server", lambda: run_compare_for_config(config)))
    for name, step in steps:
        print(f"\n--- report: {name} ---")
        try:
            step()
            written.append(name)
        except Exception as exc:  # report it, then write the others
            failed.append(name)
            print(f"The {name} report failed: {exc}")
            traceback.print_exc(limit=3)
    all_notes = list(notes or [])
    if failed:
        all_notes.append(f"Not written at {datetime.now():%Y-%m-%d %H:%M}, because of an error: {', '.join(failed)}. "
                         "The sheets for these show an earlier run, if there was one.")
    (config.filescan_folder / NOTES_FILE).write_text(json.dumps({"notes": all_notes}), encoding="utf-8")
    result = assemble_workbook(config)
    result.written, result.failed = written, failed
    return result


def print_written(result: ReportResult) -> None:
    print(f"\nWorkbook written to {result.workbook}:")
    for sheet, path, about in result.sheets:
        print(f"  {sheet:<20} {about} ({path.name})")
    for path, about in result.csv_only:
        print(f"  {'(CSV only)':<20} {about} ({path.name})")
    if result.failed:
        print(f"  Not written, because of an error: {', '.join(result.failed)}")


def run_reports(config_path: str | Path) -> ReportResult:
    result = run_reports_for_config(load_config(config_path))
    print_written(result)
    return result
