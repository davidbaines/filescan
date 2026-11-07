from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import yaml

from filescan.compare import run_compare_for_config
from filescan.config import load_config
from filescan.dedupe.hashing import full_hash
from filescan.inventory.scanner import InventoryScanner
from filescan.inventory.server_hash import ServerHasher, unc_to_posix
from filescan.storage import FileRepository, SQLiteDB
from tests.helpers.tree_builder import build_tree

_SPEC = importlib.util.spec_from_file_location(
    "fsagent", Path(__file__).resolve().parents[2] / "server" / "fsagent.py"
)
fsagent = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fsagent)


def _local_hash_runner(posix_paths: list[str]) -> list[dict]:
    """Stand-in for running fsagent hash over SSH: hash the files locally."""
    results = []
    for path in posix_paths:
        try:
            digest, _ = fsagent.hash_file(path)
            st = Path(path).stat()
            results.append({"path": path, "size": st.st_size, "mtime_ns": st.st_mtime_ns, "xxh128": digest})
        except OSError as exc:
            results.append({"path": path, "error": str(exc)})
    return results


def _setup(tmp_path: Path):
    """A PC tree and a 'server' tree on local disk, the server imported via fsagent list."""
    pc = tmp_path / "pc"
    srv = tmp_path / "srv"
    build_tree(pc, {
        "Photos/same.jpg": b"identical photo bytes",
        "Photos/renamed.jpg": b"renamed but identical",
        "Docs/edited.txt": b"version B of doc",     # same size as server copy, different bytes
        "Docs/unique.txt": b"only on the pc, unique size!!",
        "Gutenberg/book.txt": b"public domain text",  # excluded, not on server
    })
    build_tree(srv, {
        "david/Pics/same.jpg": b"identical photo bytes",
        "david/Old/other-name.jpg": b"renamed but identical",
        "david/Docs/edited.txt": b"version A of doc",
    })
    srv_posix = srv.as_posix()
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [
            {"path": str(pc)},
            {"path": "//testserver/disk3", "location": "server", "server_path": srv_posix},
        ],
        "server": {"ssh": "unused"},
        "exclusions": [{"name": "corpora", "pattern": r"\\Gutenberg\\", "reason": "Public corpus"}],
        "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"),
        "database_filename": "db.sqlite",
        "report_filename": "r.xlsx",
        "worker_count": 2,
    }))
    config = load_config(config_path)
    InventoryScanner(config, rescan=True).scan()

    from filescan.inventory.server_import import ServerImporter

    def list_runner(root_config):
        import io
        out = io.StringIO()
        fsagent.cmd_list(root_config.server_path, set(), set(), out)
        # fsagent on Windows emits backslashes; the real server emits '/'.
        return iter(line.replace("\\\\", "/") for line in out.getvalue().splitlines())

    ServerImporter(config, runner=list_runner).run()
    return config, pc, srv


def test_unc_to_posix_round_trip(tmp_path: Path) -> None:
    config, _pc, srv = _setup(tmp_path)
    root = config.root_configs[1]
    unc = Path(r"\\testserver\disk3\david\Pics\same.jpg")
    assert unc_to_posix(unc, root) == srv.as_posix() + "/david/Pics/same.jpg"
    assert unc_to_posix(Path(r"\\other\share\x"), root) is None


def test_server_hasher_imports_hashes_only_for_unchanged_files(tmp_path: Path) -> None:
    config, _pc, srv = _setup(tmp_path)
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    targets = [f for f in repo.list_active_files() if str(f.path).startswith("\\\\testserver")]
    db.close()

    def runner(root_config, posix_paths):
        results = _local_hash_runner(posix_paths)
        for r in results:  # simulate one file edited since the listing
            if r["path"].endswith("edited.txt"):
                r["mtime_ns"] += 1_000_000  # 1 ms; a float mtime cannot resolve 1 ns
        return results

    summary = ServerHasher(config, runner=runner).hash_files(targets)

    db = SQLiteDB(config.database_path)
    hashed = {f.filename: f.full_hash for f in FileRepository(db).list_active_files() if f.full_hash}
    db.close()
    assert hashed["same.jpg"] == full_hash(srv / "david/Pics/same.jpg")
    assert "edited.txt" not in hashed
    assert summary.stale == 1


def test_compare_lists_pc_files_without_identical_copy_on_server(tmp_path: Path) -> None:
    config, pc, _srv = _setup(tmp_path)

    from filescan.dedupe.hash_all import run_hash_all_for_config

    run_hash_all_for_config(config, server_runner=lambda rc, paths: _local_hash_runner(paths))
    result = run_compare_for_config(config)

    rows = {Path(r["path"]).name: r for r in csv.DictReader(result.files_csv.open(encoding="utf-8-sig"))}
    assert "same.jpg" not in rows and "renamed.jpg" not in rows  # identical copies exist on the server
    assert rows["edited.txt"]["status"] == "content-differs"
    assert rows["unique.txt"]["status"] == "no-size-match"
    assert rows["book.txt"]["excluded_by"] == "corpora"
    assert result.missing_files == 3  # edited, unique, book (excluded ones still listed)
    summary = list(csv.DictReader(result.summary_csv.open(encoding="utf-8-sig")))
    docs = next(r for r in summary if r["top_folder"].endswith("Docs"))
    assert int(docs["missing_files"]) == 2
