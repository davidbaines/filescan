from __future__ import annotations

import json
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

from filescan.config import load_config
from filescan.storage import FileRepository, SQLiteDB


def _latest_artifact(artifact_dir: Path, pattern: str) -> dict[str, object] | None:
    matches = sorted(artifact_dir.glob(pattern))
    if not matches:
        return None
    return json.loads(matches[-1].read_text())


def _column_name(index: int) -> str:
    name = ""
    current = index
    while current:
        current, remainder = divmod(current - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _cell_xml(row_index: int, column_index: int, value: object) -> str:
    cell_ref = f"{_column_name(column_index)}{row_index}"
    if value is None:
        return f'<c r="{cell_ref}"/>'
    if isinstance(value, bool):
        numeric = 1 if value else 0
        return f'<c r="{cell_ref}" t="b"><v>{numeric}</v></c>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{cell_ref}"><v>{value}</v></c>'
    return f'<c r="{cell_ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def _sheet_xml(rows: list[list[object]]) -> str:
    row_xml: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cells = "".join(_cell_xml(row_index, column_index, value) for column_index, value in enumerate(row, start=1))
        row_xml.append(f'<row r="{row_index}">{cells}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(row_xml)}</sheetData>"
        "</worksheet>"
    )


def _write_xlsx(workbook_path: Path, sheets: list[tuple[str, list[list[object]]]]) -> None:
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    content_types_overrides = [
        f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for index, _ in enumerate(sheets, start=1)
    ]
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>"
        + "".join(
            f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
            for index, (name, _) in enumerate(sheets, start=1)
        )
        + "</sheets></workbook>"
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(
            f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
            for index, _ in enumerate(sheets, start=1)
        )
        + "</Relationships>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        + "".join(content_types_overrides)
        + "</Types>"
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        "</Relationships>"
    )
    with ZipFile(workbook_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        for index, (_, rows) in enumerate(sheets, start=1):
            archive.writestr(f"xl/worksheets/sheet{index}.xml", _sheet_xml(rows))


def write_report(config_path: str | Path) -> Path:
    
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    summary_rows: list[list[object]] = [["Root", "Scan Run", "Total Folders", "Total Files", "Indexed Folders", "Indexed Files"]]
    for row in repo.list_scan_stats():
        summary_rows.append(
            [
                row["root_path"],
                row["scan_run_id"],
                row["total_folders"],
                row["total_files"],
                row["indexed_folders"],
                row["indexed_files"],
            ]
        )

    duplicates_rows: list[list[object]] = [["full_hash", "size_bytes", "file_count", "total_bytes", "paths"]]
    for group in repo.list_duplicate_groups():
        duplicates_rows.append(
            [
                group.full_hash,
                group.size_bytes,
                group.file_count,
                group.total_bytes,
                "\n".join(str(file_record.path) for file_record in group.files),
            ]
        )

    similarity_rows: list[list[object]] = [
        [
            "folder_a",
            "folder_b",
            "score",
            "shared_duplicate_files",
            "shared_signatures",
            "name_bonus",
            "reason",
        ]
    ]
    for candidate in repo.list_similarity_candidates():
        similarity_rows.append(
            [
                str(candidate.folder_a),
                str(candidate.folder_b),
                candidate.score,
                candidate.shared_duplicate_files,
                candidate.shared_signatures,
                candidate.name_bonus,
                candidate.reason,
            ]
        )

    plan_rows: list[list[object]] = [["plan_id", "proposal_id", "action", "target_path", "approval_state", "reason"]]
    latest_plan = _latest_artifact(config.artifact_dir, "plan-*.json")
    if latest_plan:
        for proposal in latest_plan["proposals"]:
            plan_rows.append(
                [
                    latest_plan["plan_id"],
                    proposal["proposal_id"],
                    proposal["action"],
                    proposal["target_path"],
                    proposal["approval_state"],
                    proposal["reason"],
                ]
            )

    execution_rows: list[list[object]] = [
        [
            "execution_id",
            "proposal_id",
            "status",
            "files_copied",
            "bytes_copied",
            "verification_passed",
            "source_cleanup_allowed",
            "errors",
        ]
    ]
    latest_execution = _latest_artifact(config.artifact_dir, "execution-*.json")
    if latest_execution:
        for result in latest_execution["proposal_results"]:
            execution_rows.append(
                [
                    latest_execution["execution_id"],
                    result["proposal_id"],
                    result["status"],
                    result["files_copied"],
                    result["bytes_copied"],
                    result["verification_passed"],
                    result["source_cleanup_allowed"],
                    "\n".join(result["errors"]),
                ]
            )

    _write_xlsx(
        config.report_path,
        [
            ("Summary", summary_rows),
            ("Duplicates", duplicates_rows),
            ("Similarity", similarity_rows),
            ("Plans", plan_rows),
            ("Execution", execution_rows),
        ],
    )
    db.close()
    return config.report_path
