from __future__ import annotations

import asyncio
from pathlib import Path, PureWindowsPath

import yaml

from filescan.cleanup.unwanted import parse_rules
from filescan.config import load_config
from filescan.models import FileRecord, FolderRecord
from filescan.review.app import ReviewApp
from filescan.review.model import FILES, KEEP, TRASH, TREE, ReviewFile, build_units, recompute_unique
from filescan.review.session import ReviewSession
from filescan.storage import FileRepository, SQLiteDB

TRASH_ROOT = r"\\srv\disk3\.Trash-1000"


def _config(tmp_path: Path):
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({
        "roots": [{"path": str(tmp_path / "pc")}],
        "exclusions": [
            {"name": "server-trash", "pattern": r"\\\.Trash-1000\\", "reason": "Server Trash"},
            {"name": "acronis", "pattern": r"\\Acronis[^\\]*\\", "reason": "Disk images"},
            {"name": "old-os-in-backups", "pattern": r"\\(Program Files|ProgramData)\\", "reason": "Old OS"},
            {"name": "evelyn", "pattern": r"\\evelyn\\", "reason": "Out of scope", "disposition": "keep-in-place"},
        ],
        "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"),
        "database_filename": "db.sqlite",
        "report_filename": "r.xlsx",
    }))
    return load_config(path)


def _add(repo: FileRepository, files: dict[str, tuple[int, str | None]]) -> None:
    """Insert files given as {path: (size, full_hash)}, creating their folders."""
    by_folder: dict[str, list[tuple[str, int, str | None]]] = {}
    for path, (size, digest) in files.items():
        by_folder.setdefault(str(PureWindowsPath(path).parent), []).append((path, size, digest))
    run = repo.begin_scan_run()
    for folder, entries in by_folder.items():
        fp = Path(folder)
        repo.upsert_folder(FolderRecord(path=fp, drive=fp.drive, parent_path=fp.parent, depth=len(fp.parts),
                                        file_count=len(entries), total_bytes=sum(e[1] for e in entries),
                                        mtime=1.0, scan_run_id=run))
        repo.upsert_files(fp, [FileRecord(path=Path(p), folder_path=fp, filename=Path(p).name, size=s,
                                          mtime=1.0, ctime=1.0) for p, s, _ in entries], run)
    for path, (_, digest) in files.items():
        if digest:
            row = repo.db.conn.execute("SELECT id FROM files WHERE path = ?", (str(Path(path)),)).fetchone()
            repo.upsert_file_hash(int(row["id"]), full_hash=digest)


def _session(tmp_path: Path, files: dict, **kwargs) -> tuple[ReviewSession, SQLiteDB]:
    config = _config(tmp_path)
    db = SQLiteDB(config.database_path)
    _add(FileRepository(db), files)
    session = ReviewSession(db, config, "server-trash", unwanted_rules=[], **kwargs)
    session.load()
    return session, db


T = TRASH_ROOT + r"\files"


def test_copy_in_doomed_area_does_not_count_as_kept(tmp_path):
    session, db = _session(tmp_path, {
        T + r"\a\kept.jpg": (100, "h-kept"),
        T + r"\a\doomed.jpg": (200, "h-doomed"),
        T + r"\a\evelyn.jpg": (300, "h-ev"),
        T + r"\a\nohash.bin": (400, None),
        r"C:\pc\Photos\kept.jpg": (100, "h-kept"),
        r"C:\pc\Acronis Backups\doomed.jpg": (200, "h-doomed"),
        r"\\srv\disk3\evelyn\evelyn.jpg": (300, "h-ev"),
    })
    try:
        files = {Path(f.path).name: f.unique for f in session.files_by_root[session.roots[0]]}
        assert files == {"kept.jpg": False, "doomed.jpg": True, "evelyn.jpg": False, "nohash.bin": True}
        assert session.roots == [T + r"\a"]  # the synthetic database has only leaf folders
    finally:
        db.close()


def _files(root: str, spec: dict[str, int]) -> list[ReviewFile]:
    return [ReviewFile(root + "\\" + rel, (root + "\\" + rel).rsplit("\\", 1)[0], size, f"h{i}")
            for i, (rel, size) in enumerate(spec.items())]


def test_large_trees_split_into_children_and_files_here():
    root = r"\\s\t"
    files = _files(root, {r"big\a\1.bin": 60, r"big\b\2.bin": 50, r"big\top.bin": 5, r"small\3.bin": 10})
    units = build_units(root, files, split_bytes=100)
    keys = sorted((u.path[len(root):], u.kind) for u in units)
    assert keys == [(r"\big", FILES), (r"\big\a", TREE), (r"\big\b", TREE), (r"\small", TREE)]


def test_suggested_tree_is_not_split():
    root = r"\\s\t"
    files = _files(root, {r"Program Files\a\1.bin": 600, r"Program Files\b\2.bin": 500})
    units = build_units(root, files, split_bytes=100,
                        suggest=lambda p, k: object() if p.endswith("Program Files") else None)
    assert [(u.path, u.kind) for u in units] == [(root + r"\Program Files", TREE)]


def test_keep_decision_removes_shared_content_from_other_units():
    root = r"\\s\t"
    a = ReviewFile(root + r"\a\x.jpg", root + r"\a", 10, "same")
    b = ReviewFile(root + r"\b\x.jpg", root + r"\b", 10, "same")
    units = build_units(root, [a, b], split_bytes=5)
    recompute_unique(units)
    assert [u.unique_bytes for u in units] == [10, 10]
    units[0].decision = KEEP
    recompute_unique(units)
    assert [u.unique_bytes for u in units] == [10, 0]


def test_decisions_persist_and_resume(tmp_path):
    files = {T + rf"\d{i}\f.bin": (10 + i, None) for i in range(3)}
    files[T + r"\d0\sub\g.bin"] = (5, None)
    session, db = _session(tmp_path, files, split_gb=1e-8)
    try:
        by_path = {u.path: u for u in session.units}
        session.decide(by_path[T + r"\d1"], TRASH)
        session.decide(by_path[T + r"\d2"], KEEP)
        db.close()
        db = SQLiteDB(session.config.database_path)
        again = ReviewSession(db, session.config, "server-trash", unwanted_rules=[], split_gb=1e-8)
        again.load()
        decisions = {u.path: u.decision for u in again.units}
        assert decisions[T + r"\d1"] == TRASH
        assert decisions[T + r"\d2"] == KEEP
        assert decisions[T + r"\d0\sub"] is None
        out = again.export_csv()
        assert "trash" in out.read_text(encoding="utf-8-sig")
    finally:
        db.close()


def test_review_app_keys_record_decisions(tmp_path):
    files = {T + r"\big\f.bin": (500, None), T + r"\small\f.bin": (100, None)}
    session, db = _session(tmp_path, files, split_gb=1e-7)
    opened: list[str] = []

    async def drive() -> None:
        app = ReviewApp(session, open_command=["cmd", "/c", "rem"])
        async with app.run_test() as pilot:
            await pilot.press("t")       # trash the largest (selected first)
            await pilot.press("k")       # moves on automatically: keep the next
            await pilot.press("u")       # undo that keep
            await pilot.pause()
            opened.append(app._current().path)

    try:
        asyncio.run(drive())
        stored = session.stored_decisions()
        assert stored == {((T + r"\big").lower(), TREE): TRASH}
        assert opened == [T + r"\small"]
    finally:
        db.close()


def test_unwanted_rules_parse_and_match():
    rules = parse_rules({"rules": [
        {"name": "caches", "match": "folder", "pattern": r"\\(Cache|GPUCache)\\", "disposition": "delete-candidate"},
        {"name": "takeout-json", "match": "file", "within": r"\\Takeout\\", "pattern": r"\.json$"},
    ]})
    caches, takeout = rules
    folder = r"C:\Users\D\Chrome\Cache\Data\x"
    assert (folder + "\\")[: caches.folder_match_end(folder)] == r"C:\Users\D\Chrome\Cache"
    assert takeout.matches_file(r"F:\Takeout\Google Photos", "a.jpg.json")
    assert not takeout.matches_file(r"F:\Code", "package.json")


def test_folder_rule_must_end_with_separator():
    import pytest

    with pytest.raises(ValueError):
        parse_rules({"rules": [{"name": "x", "match": "folder", "pattern": r"\\Cache"}]})


def test_accept_all_needs_a_second_press(tmp_path):
    files = {T + r"\Program Files\a.bin": (50, None), T + r"\Docs\b.txt": (10, None)}
    session, db = _session(tmp_path, files, split_gb=1e-8)

    async def drive() -> None:
        app = ReviewApp(session, open_command=["cmd", "/c", "rem"])
        async with app.run_test() as pilot:
            await pilot.press("A")
            await pilot.pause()
            assert session.stored_decisions() == {}
            await pilot.press("A")
            await pilot.pause()

    try:
        asyncio.run(drive())
        assert session.stored_decisions() == {((T + r"\Program Files").lower(), TREE): TRASH}
    finally:
        db.close()


def test_version_rule_matches_all_but_the_newest(tmp_path):
    from filescan.cleanup.unwanted import run_unwanted_report_for_config

    config = _config(tmp_path)
    db = SQLiteDB(config.database_path)
    base = str(tmp_path / "pc" / "Docs")
    _add(FileRepository(db), {
        base + r"\report (2017_02_21 10_00_00 UTC).docx": (10, "v1"),
        base + r"\report (2017_02_23 09_00_00 UTC).docx": (20, "v3"),
        base + r"\report (2017_02_22 10_00_00 UTC).docx": (30, "v2"),
        base + r"\other (2017_02_21 10_00_00 UTC).txt": (5, "o1"),
        base + r"\plain.txt": (7, "p"),
    })
    db.close()
    rules = parse_rules({"rules": [{"name": "fh", "match": "version",
                                    "pattern": r" \((\d{4}_\d\d_\d\d \d\d_\d\d_\d\d) UTC\)"}]})
    result = run_unwanted_report_for_config(config, rules)
    assert result.by_rule["fh"] == (2, 40, 40)
    listed = result.files_csv.read_text(encoding="utf-8-sig")
    assert "2017_02_21 10_00_00 UTC).docx" in listed and "2017_02_22" in listed
    assert "2017_02_23" not in listed and "other" not in listed


def test_measure_counts_distinct_content_and_unwanted_levels(tmp_path):
    from filescan.reporting.measure import DELETE, NORMAL, REVIEW, measure

    config = _config(tmp_path)
    db = SQLiteDB(config.database_path)
    try:
        _add(FileRepository(db), {
            r"C:\pc\Photos\a.jpg": (100, "same"),
            r"\\srv\disk3\photos\a-copy.jpg": (100, "same"),       # both places, counted once
            r"C:\pc\Docs\only-pc.txt": (10, None),
            r"\\srv\disk3\music\only-server.mp3": (20, "srv"),
            r"C:\pc\Acronis\image.tib": (1000, "img"),               # excluded
            r"C:\pc\Downloads\setup.msi": (30, "msi"),               # unwanted: review
            r"C:\pc\Cache\blob": (40, "cache"),                       # unwanted: delete-candidate
        })
        ids = {row["path"]: row["id"] for row in db.conn.execute("SELECT id, path FROM files")}
        db.conn.executemany("INSERT INTO unwanted_matches VALUES (?, ?, ?)", [
            (ids[r"C:\pc\Downloads\setup.msi"], "installers", "review"),
            (ids[r"C:\pc\Cache\blob"], "caches", "delete-candidate"),
        ])
        db.conn.commit()
        result = measure(db, config)
    finally:
        db.close()
    assert result.distinct[DELETE] == [80, 20, 100]
    assert result.distinct[REVIEW] == [40, 20, 100]
    assert result.distinct[NORMAL] == [10, 20, 100]
    assert result.excluded_files == 1


def test_short_path_keeps_drive_and_end():
    from filescan.review.model import short_path

    assert short_path(r"C:\Users\David\Pictures") == r"C:\Users\David\Pictures"
    assert short_path(r"\\192.168.1.133\disk3\photos\2016") == r"disk3\photos\2016"
    long = r"\\192.168.1.133\disk3\.Trash-1000\files\E6510-first-disk\backup.0\kdrive\Users\David\VirtualBox VMs\Win7"
    shown = short_path(long, width=30)
    assert shown.startswith("disk3\\\u2026\\") and shown.endswith(r"VirtualBox VMs\Win7")


def test_copy_matched_by_an_unwanted_rule_does_not_count_as_kept(tmp_path):
    config = _config(tmp_path)
    db = SQLiteDB(config.database_path)
    _add(FileRepository(db), {
        T + r"\a\mail.mbox": (100, "h-mail"),
        T + r"\a\doc.txt": (50, "h-doc"),
        r"C:\pc\Backup\mail (2017_02_21 10_00_00 UTC).mbox": (100, "h-mail"),   # an older version
        r"C:\pc\Docs\doc.txt": (50, "h-doc"),
    })
    old = db.conn.execute("SELECT id FROM files WHERE path LIKE '%2017_02_21%'").fetchone()["id"]
    db.conn.execute("INSERT INTO unwanted_matches VALUES (?, 'file-history-old-versions', 'review')", (old,))
    db.conn.commit()
    session = ReviewSession(db, config, "server-trash", unwanted_rules=[])
    session.load()
    try:
        unique = {Path(f.path).name: f.unique for f in session.files_by_root[session.roots[0]]}
        assert unique == {"mail.mbox": True, "doc.txt": False}
        assert all(u.suggestion is None or "kept location" not in u.suggestion.reason for u in session.units)
    finally:
        db.close()
