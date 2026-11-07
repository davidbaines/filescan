from __future__ import annotations

import os
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import yaml

from filescan.cleanup.plan import PlanEntry, Plan, build_plan, effective_decision, read_plan, write_plan
from filescan.cleanup.quarantine import fsagent, last_states, purge, quarantine, restore
from filescan.config import load_config
from filescan.dedupe.hashing import full_hash
from filescan.inventory.scanner import run_scan
from filescan.models import QUARANTINE_DIR
from filescan.review.model import FILES, KEEP, TRASH, TREE
from filescan.storage import SQLiteDB
from tests.helpers.tree_builder import build_tree


def _setup(tmp_path: Path):
    pc = tmp_path / "pc"
    build_tree(pc, {
        "Trash/old/a.txt": b"unique A",
        "Trash/old/dup.txt": b"kept elsewhere",
        "Trash/old/keepme/b.txt": b"marked keep",
        "Trash/x/t.txt": b"two trashed copies",
        "Trash/y/t.txt": b"two trashed copies",
        "Trash/filesdir/f.txt": b"files here",
        "Trash/filesdir/sub/g.txt": b"in a subfolder",
        "Trash/undecided/u.txt": b"no decision",
        "Trash/pinned/p.txt": b"pinned",
        "Docs/dup.txt": b"kept elsewhere",
        "Code/repo/.git/HEAD": b"ref",
        "Code/repo/bin/out.o": b"repo build output",
        "Code/tool/bin/x.o": b"loose build output",
        "Code/tool/bin/maybe.iso": b"review-only rule",
        "Trash/old/evelyn/e.txt": b"keep-in-place, listed after the trash rule",
        ".tool/cache/c.bin": b"a live program's cache",
    })
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}],
        "exclusions": [{"name": "server-trash", "pattern": r"\\Trash\\", "reason": "Trash"},
                       {"name": "evelyn", "pattern": r"\\evelyn\\", "disposition": "keep-in-place"}],
        "pinned": [str(pc / "Trash" / "pinned")],
        "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"),
        "database_filename": "db.sqlite",
        "report_filename": "r.xlsx",
        "min_file_size": 0,
    }))
    run_scan(config_path)
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    for row in db.conn.execute("SELECT id, path FROM files").fetchall():
        db.conn.execute("INSERT OR REPLACE INTO file_hashes (file_id, full_hash) VALUES (?, ?)",
                        (row["id"], full_hash(Path(row["path"]))))
    T = str(pc / "Trash")
    decisions = [(T + r"\old", TREE, TRASH), (T + r"\old\keepme", TREE, KEEP), (T + r"\x", TREE, TRASH),
                 (T + r"\y", TREE, TRASH), (T + r"\filesdir", FILES, TRASH), (T + r"\pinned", TREE, TRASH)]
    db.conn.executemany(
        "INSERT INTO review_decisions (scope, path, kind, decision, unique_bytes, total_bytes) VALUES "
        "('server-trash', ?, ?, ?, 0, 0)", decisions)
    for name, rule, disposition in (("out.o", "build-output", "delete-candidate"),
                                    ("x.o", "build-output", "delete-candidate"),
                                    ("c.bin", "caches", "delete-candidate"),
                                    ("maybe.iso", "disk-images", "review")):
        fid = db.conn.execute("SELECT id FROM files WHERE filename = ?", (name,)).fetchone()["id"]
        db.conn.execute("INSERT INTO unwanted_matches VALUES (?, ?, ?)", (fid, rule, disposition))
    db.conn.commit()
    return config, db, pc


def _rel(entries, pc: Path) -> dict[str, PlanEntry]:
    return {os.path.relpath(e.path, pc).replace("\\", "/"): e for e in entries}


def test_effective_decision_tree_files_and_keep():
    d = {(r"c:\t\old", TREE): TRASH, (r"c:\t\old\keep", TREE): KEEP, (r"c:\t\f", FILES): TRASH}
    assert effective_decision(r"C:\t\old\deep\er", d) == TRASH
    assert effective_decision(r"C:\t\old\keep\x", d) == KEEP
    assert effective_decision(r"C:\t\f", d) == TRASH
    assert effective_decision(r"C:\t\f\sub", d) is None          # "files here" does not reach subfolders
    assert effective_decision(r"C:\t\other", d) is None
    assert effective_decision("D:\\", {("d:", TREE): TRASH}) == TRASH


def test_plan_selects_the_right_files(tmp_path):
    config, db, pc = _setup(tmp_path)
    try:
        plan = build_plan(db, config, plan_id="p1")
    finally:
        db.close()
    got = _rel(plan.entries, pc)
    assert set(got) == {"Trash/old/a.txt", "Trash/old/dup.txt", "Trash/x/t.txt", "Trash/y/t.txt",
                        "Trash/filesdir/f.txt", "Code/tool/bin/x.o"}
    assert got["Trash/old/dup.txt"].status == "covered"
    assert got["Trash/old/dup.txt"].copy_path.endswith(r"Docs\dup.txt")
    # Two trashed copies of the same file must not cover each other.
    assert got["Trash/x/t.txt"].status == got["Trash/y/t.txt"].status == "unique"
    assert got["Trash/old/a.txt"].reason == "review:server-trash"
    assert got["Code/tool/bin/x.o"].reason == "rule:build-output"
    assert plan.held == Counter({"pinned folder": 1, "inside a Git repository": 1, "keep-in-place exclusion": 1,
                                 "live program folder (dot folder in the profile)": 1})


def test_quarantine_restore_and_purge(tmp_path):
    config, db, pc = _setup(tmp_path)
    try:
        plan = build_plan(db, config, plan_id="p1")
    finally:
        db.close()
    folder = write_plan(plan, config.filescan_folder / "cleanup")
    assert (folder / "summary.csv").exists()
    _, entries = read_plan(folder / "files.csv")
    assert len(entries) == 6

    # A file edited after the scan is left alone.
    edited = pc / "Trash" / "x" / "t.txt"
    edited.write_bytes(b"edited since the scan, now longer")
    qroot = pc / QUARANTINE_DIR / "p1"

    assert quarantine(config, folder, dry_run=True) == Counter(would_move=6)
    assert (pc / "Trash" / "old" / "a.txt").exists()

    counts = quarantine(config, folder)
    assert counts == Counter(moved=5, changed=1)
    assert edited.exists()
    assert (qroot / "Trash" / "old" / "a.txt").read_bytes() == b"unique A"
    assert not (pc / "Trash" / "old" / "a.txt").exists()
    assert not (pc / "Trash" / "y").exists()                 # emptied folders are removed
    assert (pc / "Trash" / "old" / "keepme" / "b.txt").exists()
    assert (pc / "Trash" / "filesdir" / "sub" / "g.txt").exists()
    db = SQLiteDB(config.database_path)
    missing = {r["filename"] for r in db.conn.execute("SELECT filename FROM files WHERE is_missing = 1")}
    db.close()
    assert missing == {"a.txt", "dup.txt", "t.txt", "f.txt", "x.o"}

    # A rerun moves nothing new; a rescan does not index the quarantine.
    assert quarantine(config, folder) == Counter(changed=1)
    run_scan(config.filescan_folder.parent / "config.yml")
    db = SQLiteDB(config.database_path)
    assert db.conn.execute("SELECT COUNT(*) FROM files WHERE path LIKE ?",
                           (f"%{QUARANTINE_DIR}%",)).fetchone()[0] == 0
    db.close()

    # Restore puts everything back.
    assert restore(config, folder) == Counter(moved=5)
    assert (pc / "Trash" / "old" / "a.txt").read_bytes() == b"unique A"
    assert (pc / "Trash" / "y" / "t.txt").exists()
    assert not (pc / QUARANTINE_DIR / "p1").exists()

    # Quarantine again, then purge.
    assert quarantine(config, folder)["moved"] == 5
    early = purge(config, folder, yes=True)
    assert early["in quarantine for less than the waiting period"] == 5
    assert (qroot / "Trash" / "old" / "a.txt").exists()

    later = datetime.now() + timedelta(days=15)
    (pc / "Docs" / "dup.txt").unlink()                       # the covering copy has gone
    assert purge(config, folder, now=later)["would_purge"] == 4   # without --yes nothing is deleted
    assert (qroot / "Trash" / "old" / "a.txt").exists()
    counts = purge(config, folder, yes=True, now=later)
    assert counts["purged"] == 4 and counts["its copy is gone or has changed"] == 1
    assert not (qroot / "Trash" / "old" / "a.txt").exists()
    assert (qroot / "Trash" / "old" / "dup.txt").exists()   # held: its copy is gone
    assert last_states(folder)[str(pc / "Trash" / "old" / "a.txt")]["status"] == "purged"


def test_server_files_go_through_fsagent(tmp_path):
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump({
        "roots": [{"path": r"\\srv\disk3", "location": "server", "server_path": "/srv/disk3"}],
        "server": {"ssh": "me@srv"},
        "filescan_folder": str(tmp_path / "fs"), "database_folder": str(tmp_path / "fs"),
        "database_filename": "db.sqlite", "report_filename": "r.xlsx",
    }))
    config = load_config(cfg)
    SQLiteDB(config.database_path).close()
    entry = PlanEntry(file_id=1, path=r"\\srv\disk3\.Trash-1000\files\a b.txt", location="server", size=5,
                      mtime=1.5, full_hash="", reason="review:server-trash", unit="u")
    folder = write_plan(Plan("p9", [entry]), config.filescan_folder / "cleanup")
    calls = []

    def runner(root, op, manifest):
        calls.append((op, manifest))
        return [{"src": m["src"], "dst": m["dst"], "status": "moved"} for m in manifest]

    assert quarantine(config, folder, runner=runner) == Counter(moved=1)
    op, manifest = calls[0]
    assert op == "move"
    assert manifest == [{"src": "/srv/disk3/.Trash-1000/files/a b.txt",
                         "dst": f"/srv/disk3/{QUARANTINE_DIR}/p9/.Trash-1000/files/a b.txt",
                         "stop": "/srv/disk3", "size": 5, "mtime": 1.5}]
    assert last_states(folder)[entry.path]["status"] == "moved"
    assert QUARANTINE_DIR in config.root_configs[0].exclude_dirs


def test_fsagent_move_checks_size_mtime_and_never_overwrites(tmp_path):
    root = tmp_path / "disk"
    build_tree(root, {"a/b/f.txt": b"12345", "other.txt": b"x"})
    src, dst = root / "a" / "b" / "f.txt", root / "q" / "a" / "b" / "f.txt"
    mtime = src.stat().st_mtime
    assert fsagent.move_one(str(src), str(dst), 4, mtime, str(root))["status"] == "changed"
    assert fsagent.move_one(str(src), str(dst), 5, mtime + 10, str(root))["status"] == "changed"
    assert fsagent.move_one(str(src), str(root / "other.txt"), 5, mtime, str(root))["status"] == "conflict"
    assert fsagent.move_one(str(src), str(dst), 5, mtime, str(root))["status"] == "moved"
    assert not (root / "a").exists() and root.exists()       # pruned up to, not including, stop
    assert fsagent.move_one(str(src), str(dst), 5, mtime, str(root))["note"] == "already moved"
    assert fsagent.remove_one(str(dst), 6, str(root))["status"] == "changed"
    assert fsagent.remove_one(str(dst), 5, str(root))["status"] == "purged"
    assert not (root / "q").exists() and (root / "other.txt").exists()


def test_reason_filter_and_volume_folders(tmp_path):
    config, db, pc = _setup(tmp_path)
    build_tree(pc, {"Trash/old/System Volume Information/s.dat": b"volume"})
    try:
        plan = build_plan(db, config, plan_id="p2")
    finally:
        db.close()
    folder = write_plan(plan, config.filescan_folder / "cleanup")
    assert quarantine(config, folder, dry_run=True, reason="rule:") == Counter(would_move=1)
    assert quarantine(config, folder, dry_run=True, reason="review:server-trash") == Counter(would_move=5)
    assert quarantine(config, folder, reason="rule:build-output") == Counter(moved=1)
    assert (pc / "Trash" / "old" / "a.txt").exists()


def test_a_failed_restore_leaves_the_file_restorable_and_purgeable(tmp_path):
    from filescan.cleanup.quarantine import in_quarantine

    config, pc, folder = _planned(tmp_path)
    assert quarantine(config, folder)["moved"] == 6
    original = pc / "Trash" / "old" / "a.txt"
    original.write_bytes(b"a new file in the way")       # restore will meet a conflict
    first = restore(config, folder)
    assert first["conflict"] == 1
    assert str(original) in in_quarantine(folder)        # still in quarantine, still known to be
    original.unlink()
    assert restore(config, folder)["moved"] == 1          # a second restore can finish the job
    assert original.read_bytes() == b"unique A"


def test_purge_keeps_a_file_whose_copy_changed_but_kept_its_size(tmp_path):
    config, pc, folder = _planned(tmp_path)
    assert quarantine(config, folder)["moved"] == 6
    copy = pc / "Docs" / "dup.txt"
    copy.write_bytes(b"KEPT ELSEWHERE")                   # same length, different content and date
    later = datetime.now() + timedelta(days=15)
    counts = purge(config, folder, yes=True, now=later)
    assert counts["its copy is gone or has changed"] == 1


def _planned(tmp_path: Path):
    config, db, pc = _setup(tmp_path)
    try:
        plan = build_plan(db, config, plan_id="p1")
    finally:
        db.close()
    return config, pc, write_plan(plan, config.filescan_folder / "cleanup")
