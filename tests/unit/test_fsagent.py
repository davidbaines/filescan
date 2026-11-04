from __future__ import annotations

import importlib.util
import io
import json
import os
from pathlib import Path

import pytest

from filescan.dedupe.hashing import full_hash
from tests.helpers.tree_builder import build_tree

_SPEC = importlib.util.spec_from_file_location(
    "fsagent", Path(__file__).resolve().parents[2] / "server" / "fsagent.py"
)
fsagent = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fsagent)


def _lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_jsonl(path: Path, items: list) -> None:
    path.write_text("".join(json.dumps(item) + "\n" for item in items), encoding="utf-8")


def test_hash_matches_filescan_full_hash(tmp_path: Path) -> None:
    sample = tmp_path / "sample.bin"
    sample.write_bytes(os.urandom(3 * (1 << 20) + 12345))  # spans several chunks
    assert fsagent.hash_file(str(sample))[0] == full_hash(sample)


def test_list_emits_folders_and_files_and_honours_exclusions(tmp_path: Path) -> None:
    root = tmp_path / "disk"
    build_tree(root, {"a/one.txt": b"1", "lost+found/x": b"x", "skipme/y": b"y", "two.txt": b"22"})
    out = io.StringIO()

    fsagent.cmd_list(str(root), {"lost+found"}, {os.path.abspath(str(root / "skipme"))}, out)

    records = [json.loads(line) for line in out.getvalue().splitlines()]
    files = {Path(r["path"]).name: r for r in records if r["type"] == "f"}
    folders = {Path(r["path"]).name for r in records if r["type"] == "d"}
    assert set(files) == {"one.txt", "two.txt"}
    assert files["two.txt"]["size"] == 2
    assert files["two.txt"]["mtime_ns"] == (root / "two.txt").stat().st_mtime_ns
    assert folders == {"disk", "a"}


def test_hash_appends_results_and_resumes(tmp_path: Path) -> None:
    root = tmp_path / "d"
    build_tree(root, {"a.bin": b"aaaa", "b.bin": b"bbbb"})
    paths = tmp_path / "paths.jsonl"
    out = tmp_path / "hashes.jsonl"
    _write_jsonl(paths, [str(root / "a.bin"), str(root / "b.bin"), str(root / "missing.bin")])
    # Simulate an interrupted earlier run: a.bin finished, then a torn line.
    out.write_text(
        json.dumps({"path": str(root / "a.bin"), "size": 4, "mtime_ns": 0, "xxh128": "earlier"}) + "\n{\"path\": \"tor",
        encoding="utf-8",
    )

    fsagent.cmd_hash(str(paths), str(out))

    text = out.read_text(encoding="utf-8")
    records = [json.loads(line) for line in text.splitlines() if line.startswith("{") and line.endswith("}")]
    by_name = {Path(r["path"]).name: r for r in records}
    assert by_name["a.bin"]["xxh128"] == "earlier"  # not re-hashed
    assert by_name["b.bin"]["xxh128"] == full_hash(root / "b.bin")
    assert "error" in by_name["missing.bin"]


def test_copy_verifies_preserves_mtime_and_resumes(tmp_path: Path) -> None:
    src_root = tmp_path / "src"
    build_tree(src_root, {"photos/p1.jpg": b"photo-one", "docs/d1.txt": b"document"})
    old = 1_500_000_000
    os.utime(src_root / "photos/p1.jpg", (old, old))
    dst_root = tmp_path / "dst"
    manifest = tmp_path / "manifest.jsonl"
    log = tmp_path / "log.jsonl"
    entries = [
        {"src": str(src_root / "photos/p1.jpg"), "dst": str(dst_root / "Photos/p1.jpg"),
         "xxh128": full_hash(src_root / "photos/p1.jpg")},
        {"src": str(src_root / "docs/d1.txt"), "dst": str(dst_root / "Documents/d1.txt"),
         "xxh128": full_hash(src_root / "docs/d1.txt")},
    ]
    _write_jsonl(manifest, entries)

    assert fsagent.cmd_copy(str(manifest), str(log), dry_run=False) == 0
    assert (dst_root / "Photos/p1.jpg").read_bytes() == b"photo-one"
    assert (dst_root / "Photos/p1.jpg").stat().st_mtime == pytest.approx(old)
    assert not list(dst_root.rglob("*.partial"))
    assert [r["status"] for r in _lines(log)] == ["copied", "copied"]

    # A rerun skips everything already copied.
    assert fsagent.cmd_copy(str(manifest), str(log), dry_run=False) == 0
    assert len(_lines(log)) == 2


def test_copy_never_overwrites_and_rejects_hash_mismatch(tmp_path: Path) -> None:
    src_root = tmp_path / "src"
    build_tree(src_root, {"a.txt": b"new content", "b.txt": b"bbb", "c.txt": b"same"})
    dst_root = tmp_path / "dst"
    build_tree(dst_root, {"a.txt": b"different", "c.txt": b"same"})
    manifest = tmp_path / "manifest.jsonl"
    log = tmp_path / "log.jsonl"
    _write_jsonl(manifest, [
        {"src": str(src_root / "a.txt"), "dst": str(dst_root / "a.txt"), "xxh128": full_hash(src_root / "a.txt")},
        {"src": str(src_root / "b.txt"), "dst": str(dst_root / "b.txt"), "xxh128": "0" * 32},
        {"src": str(src_root / "c.txt"), "dst": str(dst_root / "c.txt"), "xxh128": full_hash(src_root / "c.txt")},
    ])

    assert fsagent.cmd_copy(str(manifest), str(log), dry_run=False) == 1

    statuses = {Path(r["dst"]).name: r["status"] for r in _lines(log)}
    assert statuses == {"a.txt": "conflict", "b.txt": "error", "c.txt": "exists"}
    assert (dst_root / "a.txt").read_bytes() == b"different"
    assert not (dst_root / "b.txt").exists()
    assert not (dst_root / "b.txt.partial").exists()


def test_copy_dry_run_writes_nothing(tmp_path: Path) -> None:
    src = tmp_path / "s.txt"
    src.write_bytes(b"x")
    manifest = tmp_path / "m.jsonl"
    log = tmp_path / "l.jsonl"
    _write_jsonl(manifest, [{"src": str(src), "dst": str(tmp_path / "out/s.txt"), "xxh128": full_hash(src)}])

    fsagent.cmd_copy(str(manifest), str(log), dry_run=True)

    assert not (tmp_path / "out").exists()
    assert _lines(log)[0]["status"] == "would_copy"
