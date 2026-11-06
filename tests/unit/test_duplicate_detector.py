from __future__ import annotations

from pathlib import Path

from filescan.dedupe import duplicates as duplicate_module
from filescan.dedupe.duplicates import DuplicateDetector
from filescan.dedupe.hashing import quick_hash as real_quick_hash
from filescan.models import ScanConfig
from tests.helpers.indexing import index_paths
from tests.helpers.tree_builder import build_tree


def test_duplicate_detector_groups_renamed_duplicates(tmp_path: Path, make_repo) -> None:
    root = tmp_path / "root"
    created = build_tree(
        root,
        {
            "one/original.bin": b"abcdefgh12345678",
            "two/renamed.bin": b"abcdefgh12345678",
            "three/other.bin": b"zzzzzzzz99999999",
        },
    )
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "duplicates.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=8,
        )
    )

    groups = detector.run()

    assert len(groups) == 1
    assert sorted(file_record.filename for file_record in groups[0].files) == ["original.bin", "renamed.bin"]
    db.close()


def test_duplicate_detector_rejects_quick_hash_false_positive(tmp_path: Path, make_repo) -> None:
    root = tmp_path / "root"
    shared_prefix = b"a" * 4096
    shared_suffix = b"z" * 4096
    created = build_tree(
        root,
        {
            "first/collision.bin": shared_prefix + (b"b" * 4096) + shared_suffix,
            "second/collision-copy.bin": shared_prefix + (b"c" * 4096) + shared_suffix,
        },
    )
    db, repo = make_repo(tmp_path / "collision.db")
    index_paths(repo, root, created)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "collision.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=8,
        )
    )

    groups = detector.run()

    assert groups == []
    db.close()


def test_duplicate_detector_skips_unreadable_files_and_continues(tmp_path: Path, make_repo, monkeypatch) -> None:
    root = tmp_path / "root"
    created = build_tree(
        root,
        {
            "one/original.bin": b"abcdefgh12345678",
            "two/renamed.bin": b"abcdefgh12345678",
            "three/locked.bin": b"zzzzzzzz99999999",
        },
    )
    db, repo = make_repo(tmp_path / "permission.db")
    index_paths(repo, root, created)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "permission.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=8,
        )
    )

    def guarded_quick_hash(path: Path) -> str:
        if path.name == "locked.bin":
            raise PermissionError("simulated access denied")
        return real_quick_hash(path)

    monkeypatch.setattr(duplicate_module, "quick_hash", guarded_quick_hash)

    groups = detector.run()

    assert len(groups) == 1
    assert sorted(file_record.filename for file_record in groups[0].files) == ["original.bin", "renamed.bin"]
    db.close()


def test_duplicate_detector_streams_size_groups_without_loading_all_files(tmp_path: Path, make_repo, monkeypatch) -> None:
    from filescan.storage.repositories import FileRepository

    root = tmp_path / "root"
    created = build_tree(
        root,
        {
            "a/tiny.txt": b"x",
            "b/tiny-copy.txt": b"x",
            "c/unique.txt": b"only one of these",
            "d/other.txt": b"y",
        },
    )
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)

    def refuse_full_load(self, *, min_size: int = 0):
        raise AssertionError("duplicate detection must not load every file")

    monkeypatch.setattr(FileRepository, "iter_active_files", refuse_full_load)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "duplicates.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=1,
        )
    )

    groups = detector.run()

    assert [sorted(f.filename for f in g.files) for g in groups] == [["tiny-copy.txt", "tiny.txt"]]
    quick = db.conn.execute("SELECT COUNT(*) FROM file_hashes WHERE quick_hash IS NOT NULL").fetchone()[0]
    full = db.conn.execute("SELECT COUNT(*) FROM file_hashes WHERE full_hash IS NOT NULL").fetchone()[0]
    assert quick == 3  # the three 1-byte files; the unique-size file is never read
    assert full == 2  # the quick hash already separates "y" from the two "x" files
    db.close()


def test_duplicate_detector_batches_hash_commits(tmp_path: Path, make_repo, monkeypatch) -> None:
    from filescan.storage.db import SQLiteDB

    root = tmp_path / "root"
    spec = {}
    for i in range(40):  # 40 size groups of two identical files each
        content = bytes([65 + (i % 26)]) * (i + 1)
        spec[f"a/{i}.bin"] = content
        spec[f"b/{i}.bin"] = content
    created = build_tree(root, spec)
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    db.close()

    commits = {"count": 0}
    original_init = SQLiteDB.__init__

    class CountingConnection:
        def __init__(self, conn):
            self._conn = conn

        def commit(self):
            commits["count"] += 1
            return self._conn.commit()

        def __getattr__(self, name):
            return getattr(self._conn, name)

    def counting_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.conn = CountingConnection(self.conn)

    monkeypatch.setattr(SQLiteDB, "__init__", counting_init)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "duplicates.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=1,
        )
    )

    groups = detector.run()

    assert len(groups) == 40
    assert commits["count"] < 20  # 160 hashes written; one commit each would be 160+


def test_duplicate_detector_never_hashes_cloud_placeholders(tmp_path: Path, make_repo) -> None:
    root = tmp_path / "root"
    created = build_tree(
        root,
        {
            "local/a.bin": b"same content",
            "local/b.bin": b"same content",
            "cloud/c.bin": b"same content",
        },
    )
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    # Simulate an iCloud/OneDrive placeholder: reading it would download it.
    db.conn.execute("UPDATE files SET attributes = 0x400000 WHERE filename = 'c.bin'")
    db.conn.commit()
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "duplicates.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=1,
        )
    )

    groups = detector.run()

    assert [sorted(f.filename for f in g.files) for g in groups] == [["a.bin", "b.bin"]]
    hashed = {row[0] for row in db.conn.execute(
        "SELECT f.filename FROM file_hashes h JOIN files f ON f.id = h.file_id"
    )}
    assert "c.bin" not in hashed
    db.close()


def test_no_write_transaction_is_left_open_between_batches(tmp_path, write_config):
    """Hashes wait in memory, so other filescan commands are never locked out mid-run."""
    from filescan.dedupe.duplicates import DuplicateDetector
    from filescan.models import FileRecord
    from filescan.storage import FileRepository, SQLiteDB
    from filescan.config import load_config

    folder = tmp_path / "data"
    folder.mkdir()
    (folder / "a.bin").write_bytes(b"x" * 64)
    (folder / "b.bin").write_bytes(b"y" * 64)
    config = load_config(write_config([folder]))
    from filescan.inventory.scanner import InventoryScanner

    InventoryScanner(config, rescan=True).scan()
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    try:
        records = repo.files_of_size(64)
        detector = DuplicateDetector(config)
        from concurrent.futures import ThreadPoolExecutor

        class _Bar:
            def update(self, _n):
                pass

        with ThreadPoolExecutor(max_workers=2) as pool:
            detector._populate_hashes(repo, records, full=False, bar=_Bar(), pool=pool)
        assert not db.conn.in_transaction
        assert len(detector._pending) == 2  # not yet due: under 500 rows and 5 seconds
        detector._maybe_commit(repo, force=True)
        stored = db.conn.execute("SELECT COUNT(*) FROM file_hashes WHERE quick_hash IS NOT NULL").fetchone()[0]
        assert stored == 2 and not db.conn.in_transaction
    finally:
        db.close()


def _config(tmp_path: Path, root: Path, db_name: str) -> ScanConfig:
    return ScanConfig(
        roots=[root],
        filescan_folder=tmp_path / "filescan",
        database_path=tmp_path / db_name,
        report_path=tmp_path / "report.xlsx",
        duplicate_size_threshold=1,
    )


def test_second_run_reads_nothing_and_records_what_it_read(tmp_path: Path, make_repo, monkeypatch) -> None:
    import json

    root = tmp_path / "root"
    created = build_tree(root, {"a/x.bin": b"same", "b/y.bin": b"same", "c/z.bin": b"diff"})
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    config = _config(tmp_path, root, "duplicates.db")

    DuplicateDetector(config).run()
    first = json.loads(duplicate_module.summary_path(config).read_text(encoding="utf-8"))
    assert first["files"] == 3 and first["unhashed_before"] == 3
    assert first["quick_read"] == 3 and first["full_read"] == 2 and first["unreadable"] == 0

    def refuse(path: Path) -> str:
        raise AssertionError(f"{path} already has a hash and must not be read again")

    monkeypatch.setattr(duplicate_module, "quick_hash", refuse)
    monkeypatch.setattr(duplicate_module, "full_hash", refuse)
    groups = DuplicateDetector(config, rescan=True).run()
    assert [sorted(f.filename for f in g.files) for g in groups] == [["x.bin", "y.bin"]]
    second = json.loads(duplicate_module.summary_path(config).read_text(encoding="utf-8"))
    assert second["unhashed_before"] == 0 and second["quick_read"] == 0 and second["full_read"] == 0
    db.close()


def test_files_with_a_full_hash_are_not_given_a_quick_hash(tmp_path: Path, make_repo, monkeypatch) -> None:
    """compare and the server branch store full hashes only; a quick hash on top would be a wasted read."""
    from filescan.dedupe.hashing import full_hash as real_full_hash

    root = tmp_path / "root"
    created = build_tree(root, {"a/x.bin": b"same", "b/y.bin": b"same", "c/z.bin": b"diff"})
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    for record in repo.files_of_size(4):
        repo.upsert_file_hash(record.id, full_hash=real_full_hash(record.path))

    def refuse(path: Path) -> str:
        raise AssertionError(f"{path} already has a full hash")

    monkeypatch.setattr(duplicate_module, "quick_hash", refuse)
    monkeypatch.setattr(duplicate_module, "full_hash", refuse)
    groups = DuplicateDetector(_config(tmp_path, root, "duplicates.db")).run()
    assert [sorted(f.filename for f in g.files) for g in groups] == [["x.bin", "y.bin"]]
    assert db.conn.execute("SELECT COUNT(*) FROM file_hashes WHERE quick_hash IS NOT NULL").fetchone()[0] == 0
    db.close()


def test_unreadable_files_are_counted(tmp_path: Path, make_repo, monkeypatch) -> None:
    import json

    root = tmp_path / "root"
    created = build_tree(root, {"a/x.bin": b"same", "b/locked.bin": b"same"})
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)

    def guarded(path: Path) -> str:
        if path.name == "locked.bin":
            raise PermissionError("simulated access denied")
        return real_quick_hash(path)

    monkeypatch.setattr(duplicate_module, "quick_hash", guarded)
    config = _config(tmp_path, root, "duplicates.db")
    DuplicateDetector(config).run()
    assert json.loads(duplicate_module.summary_path(config).read_text(encoding="utf-8"))["unreadable"] == 1
    db.close()


def test_stale_quick_hashes_do_not_split_one_content_into_two_groups(tmp_path: Path, make_repo) -> None:
    """Seen on the live database: copies with an out-of-date quick hash landed in a second bucket."""
    root = tmp_path / "root"
    created = build_tree(root, {f"{n}/f.bin": b"same content" for n in "abcd"})
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    for record in repo.files_of_size(len(b"same content")):
        if record.path.parent.name in "cd":
            repo.upsert_file_hash(record.id, quick_hash="stale")
    groups = DuplicateDetector(_config(tmp_path, root, "duplicates.db")).run()
    assert len(groups) == 1 and len(groups[0].files) == 4
    db.close()


def test_a_file_changed_since_the_scan_is_not_hashed(tmp_path: Path, make_repo) -> None:
    import json
    import os

    root = tmp_path / "root"
    created = build_tree(root, {"a/x.bin": b"same", "b/y.bin": b"same"})
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    changed = root / "b" / "y.bin"
    changed.write_bytes(b"edit")  # same size, new content and date
    os.utime(changed, (1_000_000_000, 1_000_000_000))
    config = _config(tmp_path, root, "duplicates.db")
    assert DuplicateDetector(config).run() == []
    hashed = {row[0] for row in db.conn.execute(
        "SELECT f.filename FROM file_hashes h JOIN files f ON f.id = h.file_id")}
    assert hashed == {"x.bin"}
    assert json.loads(duplicate_module.summary_path(config).read_text(encoding="utf-8"))["changed_since_scan"] == 1
    db.close()
