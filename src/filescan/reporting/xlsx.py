"""Write a simple .xlsx workbook with the standard library only (zip and XML).

Each sheet is a list of rows. The first row of each sheet is the header: it is
shown in bold and stays in view when scrolling. Numbers are written as numbers,
so Excel can sort and add them up. A sheet longer than Excel's limit of
1,048,576 rows is cut short, with a last row saying so.
"""

from __future__ import annotations

import re
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

MAX_ROWS = 1_048_576
_NUMBER = re.compile(r"-?\d+(\.\d+)?")
# Characters XML 1.0 does not allow; a file name can contain them.
_BAD_XML = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f]")

Rows = list[list[object]]


def _column(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _cell(row: int, col: int, value: object, *, header: bool) -> str:
    ref = f"{_column(col)}{row}"
    style = ' s="1"' if header else ""
    if value is None or value == "":
        return f'<c r="{ref}"{style}/>'
    if isinstance(value, str) and not header and _NUMBER.fullmatch(value) and len(value) < 16:
        value = float(value) if "." in value else int(value)
    if isinstance(value, bool):
        return f'<c r="{ref}" t="b"{style}><v>{int(value)}</v></c>'
    if isinstance(value, (int, float)):
        return f'<c r="{ref}"{style}><v>{value}</v></c>'
    text = escape(_BAD_XML.sub("?", str(value)))
    return f'<c r="{ref}" t="inlineStr"{style}><is><t xml:space="preserve">{text}</t></is></c>'


def _sheet(rows: Rows) -> str:
    if len(rows) > MAX_ROWS:
        rows = rows[:MAX_ROWS - 1] + [[f"Cut short: {len(rows) - 1:,} rows in all; see the CSV for the rest."]]
    widths = {}
    for row in rows[:200]:
        for col, value in enumerate(row, 1):
            widths[col] = min(max(widths.get(col, 8), len(str(value)) + 2), 80)
    cols = "".join(f'<col min="{c}" max="{c}" width="{w}" customWidth="1"/>' for c, w in sorted(widths.items()))
    body = "".join(
        f'<row r="{r}">' + "".join(_cell(r, c, v, header=(r == 1)) for c, v in enumerate(row, 1)) + "</row>"
        for r, row in enumerate(rows, 1))
    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" '
            'state="frozen"/></sheetView></sheetViews>'
            + (f"<cols>{cols}</cols>" if cols else "")
            + f"<sheetData>{body}</sheetData></worksheet>")


_STYLES = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
           '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
           '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font>'
           '<font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
           '<fills count="2"><fill><patternFill patternType="none"/></fill>'
           '<fill><patternFill patternType="gray125"/></fill></fills>'
           '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
           '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
           '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
           '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>'
           '</styleSheet>')


def sheet_name(name: str) -> str:
    """Excel allows 31 characters and none of []:*?/\\ in a sheet name."""
    return re.sub(r"[\[\]:*?/\\]", "-", name)[:31]


def write_workbook(path: Path, sheets: list[tuple[str, Rows]]) -> Path:
    """Write the sheets, in order, to path. Writes to a temporary file first, so an open copy is not half-replaced."""
    path.parent.mkdir(parents=True, exist_ok=True)
    names = [sheet_name(n) for n, _ in sheets]
    n = len(sheets)
    ct = "application/vnd.openxmlformats-officedocument.spreadsheetml"
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        f'<Override PartName="/xl/workbook.xml" ContentType="{ct}.sheet.main+xml"/>'
        f'<Override PartName="/xl/styles.xml" ContentType="{ct}.styles+xml"/>'
        + "".join(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="{ct}.worksheet+xml"/>'
                  for i in range(1, n + 1))
        + "</Types>")
    rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="{rel}"><sheets>'
        + "".join(f'<sheet name="{escape(name)}" sheetId="{i}" r:id="rId{i}"/>' for i, name in enumerate(names, 1))
        + "</sheets></workbook>")
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(f'<Relationship Id="rId{i}" Type="{rel}/worksheet" Target="worksheets/sheet{i}.xml"/>'
                  for i in range(1, n + 1))
        + f'<Relationship Id="rId{n + 1}" Type="{rel}/styles" Target="styles.xml"/>'
        + "</Relationships>")
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        f'<Relationship Id="rId1" Type="{rel}/officeDocument" Target="xl/workbook.xml"/></Relationships>')
    temp = path.with_suffix(".tmp.xlsx")
    with ZipFile(temp, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", _STYLES)
        for i, (_, rows) in enumerate(sheets, 1):
            archive.writestr(f"xl/worksheets/sheet{i}.xml", _sheet(rows))
    temp.replace(path)
    return path
