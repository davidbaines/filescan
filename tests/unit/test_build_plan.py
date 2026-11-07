from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
import yaml

from filescan.planning.build_plan import UNMAPPED_ORDER, BuildPlan, Candidate, place, run_build_plan
from filescan.planning.layout import LayoutError, destination_dir, label, load_layout


def _layout(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "layout.yml"
    path.write_text(body, encoding="utf-8")
    return path


def _c(file_id, path, dest, *, order=0, size=10, full_hash="", location="pc", mtime=1.0, lab="L", repo=""):
    return Candidate(file_id=file_id, path=path, size=size, mtime=mtime, full_hash=full_hash, location=location,
                     order=order, dest=dest, label=lab, repo=repo)


def _placed(candidates) -> tuple[BuildPlan, dict[str, Candidate]]:
    plan = BuildPlan("t")
    place(candidates, plan)
    return plan, {i.dest: i for i in plan.items}


# ---------------------------------------------------------------- layout.yml


def test_layout_reports_every_problem_with_its_line(tmp_path):
    path = _layout(tmp_path, (
        "root_categories: [Photos, Work]\n"
        "map:\n"
        "  E:\\Work: Work\n"
        "  E:\\Pics: Pictures\n"
        "  e:\\work: Work/Again\n"
        "  E:\\Keep\\Sub: Work/Sub\n"
        "stay_in_place:\n"
        "  - E:\\Keep\n"))
    with pytest.raises(LayoutError) as err:
        load_layout(path)
    text = str(err.value)
    assert "line 4: destination Pictures starts with 'Pictures'" in text
    assert "line 5: e:\\work is mapped twice (also on line 3)" in text
    assert "line 6: E:\\Keep\\Sub is inside stay_in_place folder E:\\Keep" in text


def test_most_specific_entry_wins_and_unmapped_goes_to_unsorted(tmp_path):
    layout = load_layout(_layout(tmp_path, (
        "root_categories: [Photos, Work]\n"
        "map:\n"
        "  E:\\Work: Work\n"
        "  E:\\Work\\Fonts: Work/Type\n"
        "  \\\\192.168.1.133\\disk3\\photos: Photos\n")))
    assert destination_dir(r"E:\Work\a\b", layout)[1] == "Work/a/b"
    assert destination_dir(r"E:\work\FONTS\x", layout)[1] == "Work/Type/x"
    assert destination_dir(r"\\192.168.1.133\disk3\photos\2015", layout)[1] == "Photos/2015"
    assert destination_dir(r"E:\Other\x", layout) == (None, "Unsorted/E/Other/x")
    assert destination_dir(r"\\192.168.1.133\disk3\david\x", layout)[1] == "Unsorted/disk3/david/x"
    assert destination_dir("D:\\", layout)[1] == "Unsorted/D"


def test_labels_tell_sources_with_the_same_last_name_apart():
    assert label(r"E:\Work") == "E-Work"
    assert label(r"F:\Data\D\Work") == "F-Data-D-Work"
    assert label(r"\\192.168.1.133\disk3\david\Documents") == "disk3-david-Documents"


# ---------------------------------------------------------------- placing


def test_same_content_is_kept_once_at_the_earlier_layout_entry():
    plan, items = _placed([
        _c(1, r"J:\Photos\a.jpg", "Photos/a.jpg", order=1, full_hash="h"),
        _c(2, r"F:\Photos\2015\a.jpg", "Photos/2015/a.jpg", order=0, full_hash="h"),
    ])
    assert list(items) == ["Photos/2015/a.jpg"]
    assert [(c.file_id, reason, kept) for c, reason, kept in plan.dropped] == \
        [(1, "same content kept elsewhere", "Photos/2015/a.jpg")]


def test_a_clash_between_sources_with_the_same_last_name_uses_the_path_label():
    plan, items = _placed([
        _c(1, r"E:\Work\report.docx", "Work/report.docx", order=0, full_hash="a", lab="E-Work"),
        _c(2, r"F:\Data\D\Work\report.docx", "Work/report.docx", order=1, full_hash="b", lab="F-Data-D-Work"),
    ])
    assert set(items) == {"Work/report.docx", "Work/report_from_F-Data-D-Work.docx"}
    assert plan.renamed == [(r"F:\Data\D\Work\report.docx", "Work/report.docx",
                             "Work/report_from_F-Data-D-Work.docx")]


def test_folders_that_differ_only_in_case_become_one_and_names_clash_regardless_of_case():
    plan, items = _placed([
        _c(1, r"E:\a\x.txt", "Docs/Letters/x.txt", order=0, full_hash="a", lab="E"),
        _c(2, r"F:\b\y.txt", "Docs/letters/y.txt", order=1, full_hash="b", lab="F"),
        _c(3, r"F:\b\X.TXT", "Docs/letters/X.TXT", order=1, full_hash="c", lab="F"),
    ])
    assert set(items) == {"Docs/Letters/x.txt", "Docs/Letters/y.txt", "Docs/Letters/X_from_F.TXT"}


def test_a_file_and_a_folder_wanting_the_same_name():
    plan, items = _placed([
        _c(1, r"E:\notes", "Docs/notes", order=0, full_hash="a", lab="E"),
        _c(2, r"F:\notes\x.txt", "Docs/notes/x.txt", order=1, full_hash="b", lab="F"),
    ])
    assert set(items) == {"Docs/notes", "Docs/notes_from_F/x.txt"}


def test_repositories_stay_out_of_the_deduplication_both_ways():
    plan, items = _placed([
        _c(1, r"F:\Photos\logo.png", "Photos/logo.png", order=0, full_hash="h"),
        _c(2, r"F:\GitHub\site\assets\logo.png", "GitHub/site/assets/logo.png", order=5, full_hash="h",
           repo=r"f:\github\site"),
        _c(3, r"F:\GitHub\site\assets\copy.png", "GitHub/site/assets/copy.png", order=5, full_hash="h",
           repo=r"f:\github\site"),
    ])
    assert set(items) == {"Photos/logo.png", "GitHub/site/assets/logo.png", "GitHub/site/assets/copy.png"}
    assert plan.dropped == []


def test_two_repositories_at_one_destination_kept_once_if_identical_else_renamed():
    same = [
        _c(1, r"F:\GitHub\tool\a.py", "GitHub/tool/a.py", order=0, full_hash="a", repo=r"f:\github\tool", lab="F-GitHub"),
        _c(2, r"E:\GitHub\tool\a.py", "GitHub/tool/a.py", order=1, full_hash="a", repo=r"e:\github\tool", lab="E-GitHub"),
    ]
    plan, items = _placed(same)
    assert list(items) == ["GitHub/tool/a.py"] and plan.dropped[0][1] == "identical repository"
    differ = [same[0], _c(2, r"E:\GitHub\tool\a.py", "GitHub/tool/a.py", order=1, full_hash="b",
                          repo=r"e:\github\tool", lab="E-GitHub")]
    plan, items = _placed(differ)
    assert set(items) == {"GitHub/tool/a.py", "GitHub/tool_from_E-GitHub/a.py"}


def test_a_server_copy_is_the_source_but_the_kept_files_date_is_used():
    plan, items = _placed([
        _c(1, r"F:\Photos\a.jpg", "Photos/a.jpg", order=0, full_hash="h", mtime=100.0),
        _c(2, r"\\192.168.1.133\disk3\photos\a.jpg", "Photos/a.jpg", order=3, full_hash="h",
           location="server", mtime=200.0),
    ])
    item = items["Photos/a.jpg"]
    assert item.winner.file_id == 1 and item.source.file_id == 2 and item.winner.mtime == 100.0


def test_files_without_a_hash_are_each_kept():
    plan, items = _placed([
        _c(1, r"E:\Downloads\a.bin", "Downloads/a.bin", order=UNMAPPED_ORDER, size=7),
        _c(2, r"F:\Downloads\a.bin", "Downloads/a.bin", order=UNMAPPED_ORDER, size=7, lab="F"),
    ])
    assert len(items) == 2 and plan.dropped == []


# ---------------------------------------------------------------- end to end


def test_build_plan_end_to_end(tmp_path, capsys):
    from filescan.dedupe.duplicates import DuplicateDetector
    from filescan.inventory.scanner import run_scan
    from filescan.config import load_config
    from filescan.storage import SQLiteDB

    pc = tmp_path / "pc"
    for rel, text in {
        "Photos/2015/a.jpg": "same", "Old/Photos/a.jpg": "same", "Old/Photos/b.jpg": "only here",
        "Junk/cache/x.tmp": "excluded", "Zotero/db.sqlite": "pinned", "Loose/n.txt": "unmapped",
        "Code/repo/.git/HEAD": "ref", "Code/repo/a.jpg": "same",
    }.items():
        (pc / rel).parent.mkdir(parents=True, exist_ok=True)
        (pc / rel).write_text(text)
    fs = tmp_path / "fs"
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}], "filescan_folder": str(fs), "database_folder": str(fs),
        "database_filename": "db.sqlite", "report_filename": "r.xlsx", "min_file_size": 0,
        "duplicate_size_threshold": 1, "pinned": [str(pc / "Zotero")],
        "exclusions": [{"name": "junk", "pattern": r"\\Junk\\", "reason": "test"}],
    }))
    (fs).mkdir()
    (fs / "layout.yml").write_text(
        "root_categories: [Photos, GitHub]\nmap:\n"
        f"  {pc / 'Photos'}: Photos\n  {pc / 'Old' / 'Photos'}: Photos\n  {pc / 'Code'}: GitHub\n"
        f"  {pc / 'Nowhere'}: Photos/None\n", encoding="utf-8")
    run_scan(config_path)
    DuplicateDetector(load_config(config_path)).run()
    out = run_build_plan(config_path)

    with (out / "manifest.csv").open(encoding="utf-8-sig") as handle:
        dests = sorted(r["dest"] for r in csv.DictReader(handle))
    origin = pc.drive.rstrip(":")
    loose = f"Unsorted/{origin}/" + "/".join(pc.parts[1:]) + "/Loose/n.txt"
    assert dests == sorted(["Photos/2015/a.jpg", "Photos/b.jpg", "GitHub/repo/.git/HEAD", "GitHub/repo/a.jpg", loose])
    meta = json.loads((out / "plan.json").read_text(encoding="utf-8"))
    assert meta["dropped"] == 1
    assert meta["left_out"] == {"exclusion: junk": 1, "pinned folder (Phase 7 mirror)": 1}
    assert meta["unused_layout_entries"] == [f"line 6: {pc / 'Nowhere'}"]
    db = SQLiteDB(load_config(config_path).database_path)
    assert db.conn.execute("SELECT COUNT(*) FROM build_manifest").fetchone()[0] == 5
    db.close()
    assert "Nothing has been copied" in capsys.readouterr().out


def test_keep_in_place_wins_over_a_review_kept_under_another_rule(tmp_path):
    from filescan.config import load_config
    from filescan.inventory.scanner import run_scan
    from filescan.storage import SQLiteDB

    pc = tmp_path / "pc"
    (pc / "Ev" / "cache").mkdir(parents=True)
    (pc / "Ev" / "cache" / "x.txt").write_text("hers")
    fs = tmp_path / "fs"
    fs.mkdir()
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}], "filescan_folder": str(fs), "database_folder": str(fs),
        "database_filename": "db.sqlite", "report_filename": "r.xlsx", "min_file_size": 0,
        "exclusions": [{"name": "caches", "pattern": r"\\cache\\"},
                       {"name": "ev", "pattern": r"\\Ev\\", "disposition": "keep-in-place"}],
    }))
    (fs / "layout.yml").write_text("root_categories: [Docs]\nmap: {}\n", encoding="utf-8")
    run_scan(config_path)
    db = SQLiteDB(load_config(config_path).database_path)
    db.conn.execute("INSERT INTO review_decisions (scope, path, kind, decision) VALUES ('caches', ?, 'tree', 'keep')",
                    (str(pc / "Ev" / "cache"),))
    db.conn.commit()
    db.close()
    meta = json.loads((run_build_plan(config_path) / "plan.json").read_text(encoding="utf-8"))
    assert meta["items"] == 0 and meta["left_out"] == {"exclusion: ev": 1}
