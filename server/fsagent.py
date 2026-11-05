#!/usr/bin/env python3
"""fsagent: list, hash and copy files on the server, next to the data.

filescan runs on the Windows PC and drives this script over SSH, so that
server files are read at local disk speed rather than over SMB. It needs only
the Python standard library and xxhash (Debian package python3-xxhash).

All output is JSON Lines, one object per line, so any byte sequence in a file
name survives the trip. Names that are not valid UTF-8 are carried as
surrogate escapes and flagged with "bad_name": true; Windows cannot represent
them, so filescan reports them rather than mapping them to UNC paths.

Commands
--------
list ROOT [--exclude-dir NAME]... [--skip PATH]...
    One line per folder ({"type": "d", ...}) and file ({"type": "f", ...}),
    written to stdout. Symlinks are never followed.

hash --input PATHS.jsonl --output HASHES.jsonl
    PATHS.jsonl holds one JSON string per line. Each result is appended to
    HASHES.jsonl as {"path", "size", "mtime_ns", "xxh128"} or {"path", "error"}.
    Paths already present in HASHES.jsonl are skipped, so a rerun resumes.

copy --manifest MANIFEST.jsonl --log LOG.jsonl [--dry-run]
    Each manifest line is {"src", "dst", "xxh128"}. A file is copied to
    "<dst>.partial", flushed to disk, hashed again from disk, given the source
    mtime, and only then renamed to dst. An existing dst is never overwritten:
    a matching hash is logged as "exists", a different one as "conflict".
    Entries already logged as "copied" or "exists" are skipped on a rerun.

move --manifest MANIFEST.jsonl --log LOG.jsonl
    Each manifest line is {"src", "dst", "size", "mtime", "stop"}. The file is
    renamed to dst only if its size and mtime (seconds) still match, and never
    across filesystems or over an existing dst. Folders left empty are then
    removed, up to but not including "stop". Used to quarantine files and to
    restore them. A file already at dst and gone from src counts as moved, so
    a rerun resumes.

remove --manifest MANIFEST.jsonl --log LOG.jsonl
    Each manifest line is {"path", "size", "stop"}. The file is deleted only if
    its size still matches; empty folders are removed up to "stop". Used to
    empty the quarantine.

hashfile PATH
    Print the xxh128 of one file, to check parity with the PC's full hash.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import time
from typing import IO, Iterator

try:
    import xxhash
except ImportError:  # "list" works without it; hashing and copying do not.
    xxhash = None

CHUNK_BYTES = 1 << 20  # must match filescan.dedupe.hashing.full_hash
PROGRESS_SECONDS = 10.0


# --- helpers -------------------------------------------------------------------


def _is_bad_name(path: str) -> bool:
    try:
        path.encode("utf-8")
    except UnicodeEncodeError:
        return True
    return False


def _emit(out: IO[str], record: dict) -> None:
    out.write(json.dumps(record, ensure_ascii=True) + "\n")


def _read_jsonl(path: str) -> Iterator[object]:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # A crash can leave a half-written last line; ignore it.
                continue


def _open_append(path: str) -> IO[str]:
    """Open a JSONL file for appending, starting on a fresh line.

    A crash can leave a torn last line without a newline; appending straight
    after it would corrupt the first new record.
    """
    needs_newline = False
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as handle:
            handle.seek(-1, os.SEEK_END)
            needs_newline = handle.read(1) != b"\n"
    out = open(path, "a", encoding="utf-8")
    if needs_newline:
        out.write("\n")
    return out


class _Progress:
    def __init__(self, label: str, total: int | None = None) -> None:
        self.label = label
        self.total = total
        self.count = 0
        self.bytes = 0
        self.started = time.monotonic()
        self.last = self.started

    def tick(self, nbytes: int = 0) -> None:
        self.count += 1
        self.bytes += nbytes
        now = time.monotonic()
        if now - self.last >= PROGRESS_SECONDS:
            self.last = now
            self.report()

    def report(self, final: bool = False) -> None:
        elapsed = max(time.monotonic() - self.started, 1e-6)
        total = f"/{self.total:,}" if self.total is not None else ""
        # Listing reads no file data, so a byte rate would be meaningless there.
        rate = "" if self.label == "list" else f"{self.bytes / elapsed / 1e6:.0f} MB/s, "
        tag = "done" if final else "..."
        print(
            f"[{self.label}] {self.count:,}{total} items, {self.bytes / 1e9:.2f} GB, "
            f"{rate}{elapsed:.0f}s {tag}",
            file=sys.stderr,
            flush=True,
        )


def _new_digest():
    if xxhash is None:
        raise SystemExit("fsagent: xxhash is not installed (sudo apt install python3-xxhash)")
    return xxhash.xxh128()


def hash_file(path: str) -> tuple[str, int]:
    """Return (xxh128 hex digest, bytes read), identical to filescan's full_hash."""
    digest = _new_digest()
    total = 0
    with open(path, "rb") as handle:
        while chunk := handle.read(CHUNK_BYTES):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


# --- list ---------------------------------------------------------------------


def cmd_list(root: str, exclude_dirs: set[str], skip: set[str], out: IO[str]) -> int:
    root = os.path.abspath(root)
    progress = _Progress("list")
    stack = [root]
    while stack:
        folder = stack.pop()
        try:
            folder_stat = os.stat(folder, follow_symlinks=False)
            entries = list(os.scandir(folder))
        except OSError as exc:
            _emit(out, {"type": "error", "path": folder, "error": str(exc)})
            continue
        _emit(out, {"type": "d", "path": folder, "mtime_ns": folder_stat.st_mtime_ns,
                    "bad_name": _is_bad_name(folder)})
        for entry in entries:
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    if entry.name in exclude_dirs or entry.path in skip:
                        continue
                    stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    st = entry.stat(follow_symlinks=False)
                    _emit(out, {"type": "f", "path": entry.path, "size": st.st_size,
                                "mtime_ns": st.st_mtime_ns, "bad_name": _is_bad_name(entry.path)})
                    progress.tick(st.st_size)
            except OSError as exc:
                _emit(out, {"type": "error", "path": entry.path, "error": str(exc)})
    progress.report(final=True)
    return 0


# --- hash ---------------------------------------------------------------------


def cmd_hash(input_path: str, output_path: str) -> int:
    # Only a successful result for the file as it is now counts as done; errors
    # are retried and files changed since their result are hashed again.
    done: dict[str, tuple[int, int]] = {}
    for rec in _read_jsonl(output_path):
        if isinstance(rec, dict) and "xxh128" in rec:
            done[rec["path"]] = (rec.get("size"), rec.get("mtime_ns"))

    def is_done(path: str) -> bool:
        if path not in done:
            return False
        try:
            st = os.stat(path, follow_symlinks=False)
        except OSError:
            return False
        return done[path] == (st.st_size, st.st_mtime_ns)

    todo = [p for p in _read_jsonl(input_path) if isinstance(p, str) and not is_done(p)]
    print(f"[hash] {len(done):,} already done, {len(todo):,} to hash", file=sys.stderr, flush=True)
    progress = _Progress("hash", total=len(todo))
    with _open_append(output_path) as out:
        for path in todo:
            try:
                before = os.stat(path, follow_symlinks=False)
                digest, nbytes = hash_file(path)
                after = os.stat(path, follow_symlinks=False)
                if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                    raise OSError("file changed while it was being hashed")
                _emit(out, {"path": path, "size": after.st_size, "mtime_ns": after.st_mtime_ns, "xxh128": digest})
            except OSError as exc:
                nbytes = 0
                _emit(out, {"path": path, "error": str(exc)})
            out.flush()
            progress.tick(nbytes)
    progress.report(final=True)
    return 0


# --- copy ---------------------------------------------------------------------


def _drop_cache(fd: int) -> None:
    if hasattr(os, "posix_fadvise"):
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass


def _copy_one(src: str, dst: str, expected: str) -> dict:
    """Copy src to dst with verification. Never overwrites an existing dst."""
    if os.path.lexists(dst):
        try:
            existing, _ = hash_file(dst)
        except OSError as exc:
            return {"status": "error", "error": f"cannot read existing destination: {exc}"}
        if existing == expected:
            return {"status": "exists"}
        return {"status": "conflict", "error": f"destination exists with hash {existing}"}

    src_stat = os.stat(src, follow_symlinks=False)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    partial = dst + ".partial"
    digest = _new_digest()
    copied = 0
    try:
        with open(src, "rb") as reader, open(partial, "wb") as writer:
            while chunk := reader.read(CHUNK_BYTES):
                digest.update(chunk)
                writer.write(chunk)
                copied += len(chunk)
            writer.flush()
            os.fsync(writer.fileno())
            _drop_cache(writer.fileno())
        if digest.hexdigest() != expected:
            raise OSError(f"source hash {digest.hexdigest()} differs from expected {expected}")
        # Read back from disk, not from what we wrote, to catch write errors.
        written, _ = hash_file(partial)
        if written != expected:
            raise OSError(f"written copy hash {written} differs from expected {expected}")
        os.chmod(partial, stat.S_IMODE(src_stat.st_mode))
        os.utime(partial, ns=(src_stat.st_atime_ns, src_stat.st_mtime_ns))
        if os.path.lexists(dst):
            raise OSError("destination appeared during copy")
        os.rename(partial, dst)
    except OSError as exc:
        try:
            os.remove(partial)
        except OSError:
            pass
        return {"status": "error", "error": str(exc)}
    return {"status": "copied", "bytes": copied}


def cmd_copy(manifest_path: str, log_path: str, dry_run: bool) -> int:
    finished = {
        (rec["src"], rec["dst"])
        for rec in _read_jsonl(log_path)
        if isinstance(rec, dict) and rec.get("status") in ("copied", "exists")
    }
    entries = [e for e in _read_jsonl(manifest_path) if isinstance(e, dict)]
    todo = [e for e in entries if (e["src"], e["dst"]) not in finished]
    print(f"[copy] {len(entries) - len(todo):,} already done, {len(todo):,} to copy", file=sys.stderr, flush=True)
    progress = _Progress("copy", total=len(todo))
    failures = 0
    with _open_append(log_path) as log:
        for entry in todo:
            if dry_run:
                result = {"status": "would_copy"}
            else:
                try:
                    result = _copy_one(entry["src"], entry["dst"], entry["xxh128"])
                except OSError as exc:
                    result = {"status": "error", "error": str(exc)}
            if result["status"] in ("error", "conflict"):
                failures += 1
            _emit(log, {"src": entry["src"], "dst": entry["dst"], **result})
            log.flush()
            progress.tick(int(result.get("bytes", 0)))
    progress.report(final=True)
    return 1 if failures else 0


# --- move and remove (quarantine) ----------------------------------------------

MTIME_TOLERANCE = 1e-3  # seconds; the PC stores mtimes as floats


def prune_empty(folder: str, stop: str) -> None:
    """Remove folder and its parents while they are empty, never stop or anything above it."""
    # join(..., "") adds exactly one trailing separator, also for a drive root such as D:\\
    inside = os.path.normcase(os.path.join(os.path.abspath(stop), ""))
    folder = os.path.abspath(folder)
    while True:
        norm = os.path.normcase(os.path.join(folder, ""))
        if norm == inside or not norm.startswith(inside):
            return
        try:
            os.rmdir(folder)  # fails unless the folder is empty
        except OSError:
            return
        folder = os.path.dirname(folder)


def move_one(src: str, dst: str, size: int, mtime: float, stop: str | None = None) -> dict:
    """Rename src to dst if it is unchanged. Never overwrites, never copies."""
    if not os.path.lexists(src):
        if os.path.lexists(dst):
            return {"status": "moved", "note": "already moved"}
        return {"status": "missing"}
    st = os.lstat(src)
    if not stat.S_ISREG(st.st_mode):
        return {"status": "changed", "error": "no longer a regular file"}
    if st.st_size != int(size) or abs(st.st_mtime_ns / 1e9 - float(mtime)) > MTIME_TOLERANCE:
        return {"status": "changed", "error": f"size {st.st_size}, mtime {st.st_mtime_ns / 1e9}"}
    if os.path.lexists(dst):
        return {"status": "conflict", "error": "destination exists"}
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        # os.rename never copies: across filesystems it fails (EXDEV), which is
        # what we want, unlike shutil.move.
        os.rename(src, dst)
    except OSError as exc:
        return {"status": "error", "error": str(exc)}
    if stop:
        prune_empty(os.path.dirname(src), stop)
    return {"status": "moved", "bytes": st.st_size}


def remove_one(path: str, size: int, stop: str | None = None) -> dict:
    if not os.path.lexists(path):
        return {"status": "missing"}
    st = os.lstat(path)
    if not stat.S_ISREG(st.st_mode) or st.st_size != int(size):
        return {"status": "changed", "error": f"size {st.st_size}"}
    try:
        os.remove(path)
    except OSError as exc:
        return {"status": "error", "error": str(exc)}
    if stop:
        prune_empty(os.path.dirname(path), stop)
    return {"status": "purged", "bytes": st.st_size}


def _run_manifest(label: str, manifest_path: str, log_path: str, key: str, action) -> int:
    finished = {
        rec[key] for rec in _read_jsonl(log_path)
        if isinstance(rec, dict) and rec.get("status") in ("moved", "purged") and key in rec
    }
    entries = [e for e in _read_jsonl(manifest_path) if isinstance(e, dict)]
    todo = [e for e in entries if e[key] not in finished]
    print(f"[{label}] {len(entries) - len(todo):,} already done, {len(todo):,} to do", file=sys.stderr, flush=True)
    progress = _Progress(label, total=len(todo))
    failures = 0
    with _open_append(log_path) as log:
        for entry in todo:
            try:
                result = action(entry)
            except OSError as exc:
                result = {"status": "error", "error": str(exc)}
            if result["status"] in ("error", "conflict"):
                failures += 1
            _emit(log, {**{k: entry[k] for k in ("src", "dst", "path") if k in entry}, **result})
            log.flush()
            progress.tick(int(result.get("bytes", 0)))
    progress.report(final=True)
    return 1 if failures else 0


def cmd_move(manifest_path: str, log_path: str) -> int:
    return _run_manifest("move", manifest_path, log_path, "src",
                         lambda e: move_one(e["src"], e["dst"], e["size"], e["mtime"], e.get("stop")))


def cmd_remove(manifest_path: str, log_path: str) -> int:
    return _run_manifest("remove", manifest_path, log_path, "path",
                         lambda e: remove_one(e["path"], e["size"], e.get("stop")))


# --- entry point --------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fsagent", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list")
    p_list.add_argument("root")
    p_list.add_argument("--exclude-dir", action="append", default=[])
    p_list.add_argument("--skip", action="append", default=[])

    p_hash = sub.add_parser("hash")
    p_hash.add_argument("--input", required=True)
    p_hash.add_argument("--output", required=True)

    p_copy = sub.add_parser("copy")
    p_copy.add_argument("--manifest", required=True)
    p_copy.add_argument("--log", required=True)
    p_copy.add_argument("--dry-run", action="store_true")

    p_move = sub.add_parser("move")
    p_move.add_argument("--manifest", required=True)
    p_move.add_argument("--log", required=True)

    p_remove = sub.add_parser("remove")
    p_remove.add_argument("--manifest", required=True)
    p_remove.add_argument("--log", required=True)

    p_one = sub.add_parser("hashfile")
    p_one.add_argument("path")

    args = parser.parse_args(argv)
    if args.command == "list":
        skip = {os.path.abspath(s) for s in args.skip}
        return cmd_list(args.root, set(args.exclude_dir), skip, sys.stdout)
    if args.command == "hash":
        return cmd_hash(args.input, args.output)
    if args.command == "copy":
        return cmd_copy(args.manifest, args.log, args.dry_run)
    if args.command == "move":
        return cmd_move(args.manifest, args.log)
    if args.command == "remove":
        return cmd_remove(args.manifest, args.log)
    if args.command == "hashfile":
        print(hash_file(args.path)[0])
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
