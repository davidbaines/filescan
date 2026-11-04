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
