"""`filescan report`: write every report, and gather them in one workbook.

`filescan scan` runs this when it finishes, so the reports always match the
latest inventory. It reads the database and writes only reports; no file is
moved or changed.

The reports, each also written as a CSV in the filescan folder:

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
has a Contents sheet and one sheet per report. The per-file lists stay as CSV
only, because they can be longer than Excel allows.
"""

from __future__ import annotations

import csv
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from filescan.cleanup.unwanted import run_unwanted_report_for_config
from filescan.compare import run_compare_for_config
from filescan.config import load_config
from filescan.models import ScanConfig
from filescan.reporting.exclusions import write_exclusions_report
from filescan.reporting.measure import run_measure_for_config
from filescan.reporting.xlsx import MAX_ROWS, Rows, write_workbook


@dataclass
class ReportResult:
    workbook: Path | None = None
    written: list[tuple[str, Path, str]] = field(default_factory=list)  # (sheet, CSV, what it holds)
    extra_csvs: list[tuple[Path, str]] = field(default_factory=list)  # per-file lists, CSV only
    failed: list[str] = field(default_factory=list)


def _read_csv(path: Path) -> Rows:
    rows: Rows = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.reader(handle):
            rows.append(row)
            if len(rows) > MAX_ROWS:
                break
    return rows


def _contents(result: ReportResult, notes: list[str]) -> Rows:
    rows: Rows = [["sheet or file", "what it holds", "path"]]
    rows += [[sheet, about, str(path)] for sheet, path, about in result.written]
    rows += [["(CSV only)", about, str(path)] for path, about in result.extra_csvs]
    rows.append([])
    rows.append([f"Written {datetime.now():%Y-%m-%d %H:%M}"])
    rows += [[note] for note in notes]
    if result.failed:
        rows.append([f"Not written, because of an error: {', '.join(result.failed)}"])
    return rows


def run_reports_for_config(config: ScanConfig, *, notes: list[str] | None = None) -> ReportResult:
    """Write each report, then the workbook. A report that fails is listed, and the others are still written."""
    result = ReportResult()
    has_server = bool(config.server_roots)

    def measure():
        path = run_measure_for_config(config)
        result.written.append(("Measure", path, "Size of the organised set against free disk space"))

    def exclusions():
        path = write_exclusions_report(config)
        result.written.append(("Exclusions", path, "Folders each exclusion leaves out of the organised set"))

    def unwanted():
        out = run_unwanted_report_for_config(config)
        result.written.append(("Unwanted", out.report_csv, "Unwanted-data rule matches, by folder"))
        result.extra_csvs.append((out.files_csv, "Every file an unwanted-data rule matches"))

    def missing():
        out = run_compare_for_config(config)
        result.written.append(("Missing from server", out.summary_csv,
                               "PC files with no identical copy on the server, by top folder"))
        result.extra_csvs.append((out.files_csv, "Every PC file with no identical copy on the server"))

    steps = [("unwanted", unwanted), ("measure", measure), ("exclusions", exclusions)]
    if has_server:
        steps.append(("missing from server", missing))
    for name, step in steps:
        print(f"\n--- report: {name} ---")
        try:
            step()
        except Exception as exc:  # report it, then write the others
            result.failed.append(name)
            print(f"The {name} report failed: {exc}")
            traceback.print_exc(limit=3)

    order = ["Measure", "Exclusions", "Unwanted", "Missing from server"]
    result.written.sort(key=lambda w: order.index(w[0]))
    sheets = [("Contents", _contents(result, notes or []))]
    sheets += [(sheet, _read_csv(path)) for sheet, path, _ in result.written]
    try:
        result.workbook = write_workbook(config.report_path, sheets)
    except PermissionError:
        # Excel keeps an open workbook locked; write a copy beside it instead.
        alternative = config.report_path.with_name(f"{config.report_path.stem}-{datetime.now():%Y%m%d-%H%M%S}.xlsx")
        result.workbook = write_workbook(alternative, sheets)
        print(f"{config.report_path} is open in another program, so the reports went to {alternative.name}.")
    return result


def print_written(result: ReportResult) -> None:
    print(f"\nReports written to {result.workbook}:")
    for sheet, path, about in result.written:
        print(f"  {sheet:<20} {about} ({path.name})")
    for path, about in result.extra_csvs:
        print(f"  {'(CSV only)':<20} {about} ({path.name})")
    if result.failed:
        print(f"  Not written, because of an error: {', '.join(result.failed)}")


def run_reports(config_path: str | Path) -> ReportResult:
    result = run_reports_for_config(load_config(config_path))
    print_written(result)
    return result
