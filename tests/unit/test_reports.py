from __future__ import annotations

import re
import zipfile
from pathlib import Path

import yaml

from filescan.cli import main
from filescan.reporting.xlsx import sheet_name, write_workbook
from tests.helpers.tree_builder import build_tree


def _sheet_names(path: Path) -> list[str]:
    with zipfile.ZipFile(path) as z:
        return re.findall(r'<sheet name="([^"]+)"', z.read("xl/workbook.xml").decode())


def _sheet_xml(path: Path, index: int) -> str:
    with zipfile.ZipFile(path) as z:
        return z.read(f"xl/worksheets/sheet{index}.xml").decode()


def test_workbook_writes_numbers_as_numbers_and_safe_sheet_names(tmp_path):
    out = write_workbook(tmp_path / "r.xlsx", [("A/B: c", [["name", "bytes"], ["x & y", "1234"], ["z", "1.5"]])])
    assert _sheet_names(out) == ["A-B- c"]
    xml = _sheet_xml(out, 1)
    assert "<v>1234</v>" in xml and "<v>1.5</v>" in xml      # numbers, not text
    assert "x &amp; y" in xml                                 # text is escaped
    assert 's="1"' in xml                                     # the header row is bold
    assert sheet_name("x" * 40) == "x" * 31


def _config(tmp_path: Path) -> Path:
    pc = tmp_path / "pc"
    build_tree(pc, {"Docs/a.txt": b"one", "Docs/setup.tmp": b"temporary", "Old/Windows/x.dll": b"old os"})
    fs = tmp_path / "fs"
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}], "filescan_folder": str(fs), "database_folder": str(fs),
        "database_filename": "db.sqlite", "report_filename": "report.xlsx", "min_file_size": 0,
        "exclusions": [{"name": "old-os", "pattern": r"\\Old\\Windows\\", "reason": "old Windows"}],
    }))
    return config_path


def test_scan_writes_the_reports_to_one_workbook_and_lists_them(tmp_path, capsys):
    config_path = _config(tmp_path)
    assert main(["--config", str(config_path), "scan"]) == 0
    out = capsys.readouterr().out
    workbook = tmp_path / "fs" / "report.xlsx"
    assert f"Workbook written to {workbook}" in out
    # No server roots, so no "Missing from server" sheet.
    assert _sheet_names(workbook) == ["Contents", "Measure", "Exclusions", "Unwanted", "Unreadable"]
    assert "old-os" in _sheet_xml(workbook, 3)                # the exclusion is on its sheet
    # Unwanted-data matches have rows. (Tests run under a Temp folder, which a rule matches as a whole.)
    assert '<row r="2"' in _sheet_xml(workbook, 4)
    assert (tmp_path / "fs" / "unwanted_files.csv").exists()  # the per-file list stays as CSV


def test_report_writes_a_dated_copy_when_the_workbook_is_open(tmp_path, monkeypatch, capsys):
    config_path = _config(tmp_path)
    assert main(["--config", str(config_path), "scan"]) == 0
    real_replace = Path.replace

    def locked(self, target):
        if Path(target).name == "report.xlsx":
            raise PermissionError(13, "The file is open in another program")
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", locked)
    capsys.readouterr()
    assert main(["--config", str(config_path), "report"]) == 0
    assert "is open in another program" in capsys.readouterr().out
    assert list((tmp_path / "fs").glob("report-*.xlsx"))
