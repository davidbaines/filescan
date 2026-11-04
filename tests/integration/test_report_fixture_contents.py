from __future__ import annotations

import json
import os
import shutil
import zipfile
from pathlib import Path
from tempfile import gettempdir
from uuid import uuid4
from xml.etree import ElementTree as ET

import yaml

from filescan.cli import main

REPORT_FIXTURE = Path(__file__).resolve().parents[1] / "manual" / "report_fixture"
REPORT_INPUT_ROOT = REPORT_FIXTURE / "input" / "root"
REPORT_OUTPUT = REPORT_FIXTURE / "output"
MAIN_NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _sheet_rows(xlsx_path: Path) -> dict[str, list[list[str]]]:
    with zipfile.ZipFile(xlsx_path) as archive:
        workbook_xml = ET.fromstring(archive.read("xl/workbook.xml"))
        sheets = workbook_xml.find("main:sheets", MAIN_NS)
        if sheets is None:
            return {}
        rows_by_sheet: dict[str, list[list[str]]] = {}
        for index, sheet in enumerate(sheets.findall("main:sheet", MAIN_NS), start=1):
            name = sheet.attrib["name"]
            sheet_xml = ET.fromstring(archive.read(f"xl/worksheets/sheet{index}.xml"))
            sheet_rows: list[list[str]] = []
            for row in sheet_xml.findall("main:sheetData/main:row", MAIN_NS):
                values: list[str] = []
                for cell in row.findall("main:c", MAIN_NS):
                    inline = cell.find("main:is/main:t", MAIN_NS)
                    value = cell.find("main:v", MAIN_NS)
                    if inline is not None and inline.text is not None:
                        values.append(inline.text)
                    elif value is not None and value.text is not None:
                        values.append(value.text)
                    else:
                        values.append("")
                sheet_rows.append(values)
            rows_by_sheet[name] = sheet_rows
    return rows_by_sheet


def test_report_contains_expected_rows_for_persistent_fixture() -> None:
    REPORT_OUTPUT.mkdir(parents=True, exist_ok=True)

    runtime_folder = Path(gettempdir()) / "filescan-report-fixture-runtime" / uuid4().hex
    if runtime_folder.exists():
        shutil.rmtree(runtime_folder, ignore_errors=True)
    runtime_folder.mkdir(parents=True, exist_ok=True)

    # Execute writes into the scanned tree, so work on a copy of the fixture input.
    input_root = runtime_folder / "root"
    shutil.copytree(REPORT_INPUT_ROOT, input_root)
    filescan_folder = runtime_folder
    config_path = runtime_folder / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [{"path": str(input_root)}],
                "filescan_folder": str(filescan_folder),
                "database_folder": str(filescan_folder),
                "database_filename": "file_index.db",
                "report_filename": "filescan_report.xlsx",
                "duplicate_size_threshold": 1,
                "similarity_threshold": 0.2,
                "merge_threshold": 0.9,
                "worker_count": 2,
            }
        )
    )

    assert main(["--config", str(config_path), "scan"]) == 0
    assert main(["--config", str(config_path), "duplicates"]) == 0
    assert main(["--config", str(config_path), "similarity"]) == 0
    assert main(["--config", str(config_path), "plan"]) == 0

    plan_artifacts = sorted(runtime_folder.glob("plan-*.json"))
    assert plan_artifacts
    plan_payload = json.loads(plan_artifacts[-1].read_text())
    assert plan_payload["clusters"], "fixture should produce at least one merge cluster"
    for cluster in plan_payload["clusters"]:
        cluster["status"] = "approved"
    plan_artifacts[-1].write_text(json.dumps(plan_payload, indent=2))

    assert main(["--config", str(config_path), "execute", "--plan", str(plan_artifacts[-1])]) == 0
    assert main(["--config", str(config_path), "report"]) == 0

    report_path = runtime_folder / "filescan_report.xlsx"
    assert report_path.exists()
    # Refresh the tracked copy for manual inspection only when asked, so routine
    # test runs leave the working tree clean.
    if os.environ.get("FILESCAN_UPDATE_FIXTURE") == "1":
        shutil.copy2(report_path, REPORT_OUTPUT / "filescan_report.xlsx")

    rows = _sheet_rows(report_path)

    assert {"Summary", "Duplicates", "Similarity", "Plans", "Execution"} <= set(rows)

    summary_rows = rows["Summary"]
    assert any(str(input_root) in row for row in summary_rows)

    duplicate_rows = rows["Duplicates"]
    assert any("Project A readme" in " ".join(row) or "data.bin" in " ".join(row) for row in duplicate_rows)
    assert any("projectA" in " ".join(row) and "projectB" in " ".join(row) for row in duplicate_rows)

    similarity_rows = rows["Similarity"]
    assert any("projectA" in " ".join(row) and "projectB" in " ".join(row) for row in similarity_rows)

    plan_rows = rows["Plans"]
    assert any("merge_cluster" in row and "approved" in row for row in plan_rows)

    execution_rows = rows["Execution"]
    assert any("completed" in row for row in execution_rows)
