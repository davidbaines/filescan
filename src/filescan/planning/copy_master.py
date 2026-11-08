"""`filescan copy-master`: copy the latest build plan to the master (disk2), and check every file.

Without --yes it copies nothing: it says how much would be copied, from where,
against the master's free space. With --yes it does three things, each of which
can be stopped and run again; finished work is read from logs in the plan
folder and skipped:

1. Server to server: files whose copy source is on the server are copied on
   the server by `fsagent copy`, which writes to a temporary name, hashes what
   reached the disk, gives the copy the kept file's date and only then renames
   it. It never overwrites a file: an existing file with the same hash counts
   as done, and a different one is reported as a conflict.
2. PC to master: the PC reads each file once, hashing it as it goes, writes it
   over the network to a temporary name, gives it the kept file's date and
   renames it. It never overwrites a file either.
3. Check: the server hashes the PC's copies where they landed, so nothing is
   read back over the network. Then every file in the plan must have a copy
   whose hash matches the plan's (or, for a file with no hash, the hash read
   during its copy). Files that do not are written to not_on_master.csv.

The copy logs (server_copy_log.jsonl, pc_copy_log.jsonl, master_check.jsonl)
are the evidence later steps use to know each file is on the master.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import os
import re
import shlex
import subprocess
import time
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path

from filescan.config import load_config
from filescan.inventory.server_hash import WORK_DIR, unc_to_posix
from filescan.inventory.server_import import FSAGENT
from filescan.models import RootConfig, ScanConfig
from filescan.planning.build_plan import master_root
from filescan.progress import SkipLog, progress_bar
from filescan.reporting.measure import _server_disks
from filescan.reporting.report import latest_build

_spec = importlib.util.spec_from_file_location("fsagent", FSAGENT)
fsagent = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fsagent)  # the same copy and hash code runs on both machines

# (manifest entries, plan folder) -> fsagent copy results; (POSIX paths) -> fsagent hash results
CopyRunner = Callable[[list[dict], Path], Iterable[dict]]
HashRunner = Callable[[list[str]], Iterable[dict]]

CHUNK = 1 << 20


# ---------------------------------------------------------------- logs


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                record = json.loads(line)
            except ValueError:
                continue  # a line cut short by a crash
            if isinstance(record, dict):
                out.append(record)
    return out


def _append(path: Path, record: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({**record, "at": datetime.now().isoformat(timespec="seconds")}) + "\n")


# ---------------------------------------------------------------- the plan's two copy lists


def server_entries(plan: Path) -> list[dict]:
    return _read_jsonl(plan / "server_copy.jsonl")


def pc_entries(plan: Path) -> list[dict]:
    with (plan / "pc_copy.csv").open(newline="", encoding="utf-8-sig") as handle:
        return [{"src": r["src"], "dst": r["dst"], "size": int(r["size"]), "mtime": float(r["mtime"]),
                 "xxh128": r["full_hash"] or None} for r in csv.DictReader(handle)]


# ---------------------------------------------------------------- runners (SSH in real use, local in tests)


def _job_name(plan: Path, what: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", f"master-{plan.name}-{what}")


def ssh_copy_runner(config: ScanConfig) -> CopyRunner:
    def run(entries: list[dict], plan: Path) -> Iterable[dict]:
        job = f"{WORK_DIR}/{_job_name(plan, 'copy')}"
        manifest, log = shlex.quote(f"{job}.manifest.jsonl"), shlex.quote(f"{job}.log.jsonl")
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=60", config.server_ssh]
        payload = "".join(json.dumps(e) + "\n" for e in entries).encode("utf-8", "surrogateescape")
        subprocess.run(ssh + [f"mkdir -p {WORK_DIR} && cat > {manifest}"], input=payload, check=True)
        with FSAGENT.open("rb") as script:
            # fsagent prints progress on stderr; a non-zero exit only means some files failed.
            subprocess.run(ssh + [config.server_python, "-", "copy", "--manifest", manifest, "--log", log],
                           stdin=script, check=False)
        out = subprocess.run(ssh + [f"cat {log}"], capture_output=True, check=True).stdout
        for line in out.decode("utf-8", "surrogateescape").splitlines():
            if line.strip().startswith("{"):
                yield json.loads(line)
    return run


def ssh_master_hash_runner(config: ScanConfig, master: RootConfig) -> HashRunner:
    from filescan.inventory.server_hash import ssh_hash_runner

    runner = ssh_hash_runner(config)
    return lambda paths: runner(master, paths)


def local_copy_runner(entries: list[dict], plan: Path) -> Iterable[dict]:
    """Run fsagent copy here: for tests, or a master on a local disk."""
    for e in entries:
        yield {"src": e["src"], "dst": e["dst"], **fsagent._copy_one(e["src"], e["dst"], e.get("xxh128"), e.get("mtime"))}


def local_hash_runner(paths: list[str]) -> Iterable[dict]:
    for path in paths:
        try:
            digest, size = fsagent.hash_file(path)
            yield {"path": path, "size": size, "xxh128": digest}
        except OSError as exc:
            yield {"path": path, "error": str(exc)}


# ---------------------------------------------------------------- copying from the PC


def copy_from_pc(src: str, dst: str, mtime: float, expected: str | None) -> dict:
    """Copy one PC file to the master over the network. Never overwrites; never reads the copy back."""
    if os.path.lexists(dst):
        return {"status": "exists"}  # checked by hash on the server afterwards
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    partial = dst + ".partial"
    digest = fsagent._new_digest()
    copied = 0
    try:
        with open(src, "rb") as reader, open(partial, "wb") as writer:
            while chunk := reader.read(CHUNK):
                digest.update(chunk)
                writer.write(chunk)
                copied += len(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        read = digest.hexdigest()
        if expected and read != expected:
            raise OSError(f"the PC file's hash {read} differs from the plan's {expected}: it changed since the scan")
        os.utime(partial, (mtime, mtime))
        if os.path.lexists(dst):
            raise OSError("destination appeared during copy")
        os.replace(partial, dst)
    except OSError as exc:
        try:
            os.remove(partial)
        except OSError:
            pass
        return {"status": "error", "error": str(exc)}
    return {"status": "copied", "bytes": copied, "xxh128": read}


# ---------------------------------------------------------------- the three steps


def _done(log: Path, key: str = "dst") -> dict[str, dict]:
    return {r[key]: r for r in _read_jsonl(log) if r.get("status") in ("copied", "exists")}


def run_server_copies(plan: Path, runner: CopyRunner) -> Counter:
    log = plan / "server_copy_log.jsonl"
    done = _done(log)
    todo = [e for e in server_entries(plan) if e["dst"] not in done]
    print(f"Server to server: {len(done):,} done before, {len(todo):,} to copy "
          f"({sum(e.get('size', 0) for e in todo) / 1e9:,.1f} GB)...")
    counts: Counter = Counter()
    if todo:
        for result in runner(todo, plan):
            if result.get("dst") in done and result.get("status") in ("copied", "exists"):
                continue
            _append(log, result)
            counts[result.get("status", "error")] += 1
    return counts


def run_pc_copies(config: ScanConfig, plan: Path) -> Counter:
    log = plan / "pc_copy_log.jsonl"
    done = _done(log)
    todo = [e for e in pc_entries(plan) if e["dst"] not in done]
    skips = SkipLog(config.filescan_folder, "copy-master")
    counts: Counter = Counter()
    with progress_bar(desc="PC to master", total=sum(e["size"] for e in todo), unit="B", unit_scale=True) as bar:
        skips.attach(bar)
        for e in todo:
            result = copy_from_pc(e["src"], e["dst"], e["mtime"], e["xxh128"])
            if result["status"] == "error":
                skips.add("copy", e["src"], result["error"])
            _append(log, {"src": e["src"], "dst": e["dst"], **result})
            counts[result["status"]] += 1
            bar.update(e["size"])
    skips.summary()
    return counts


def check_master(config: ScanConfig, plan: Path, hash_runner: HashRunner) -> dict:
    """Is every file in the plan on the master with the right hash? Writes not_on_master.csv."""
    master = master_root(config)
    server_log = _done(plan / "server_copy_log.jsonl")
    pc_log = _done(plan / "pc_copy_log.jsonl")
    check_log = plan / "master_check.jsonl"
    checked = {r["dst"]: r for r in _read_jsonl(check_log) if r.get("xxh128")}

    # The PC's copies are hashed on the server, once each.
    need = []
    for e in pc_entries(plan):
        if e["dst"] in pc_log and e["dst"] not in checked:
            posix = unc_to_posix(Path(e["dst"]), master) if master and master.location == "server" else e["dst"]
            need.append((e["dst"], posix))
    if need:
        print(f"Hashing {len(need):,} copies on the master, where they landed...")
        back = {posix: dst for dst, posix in need}
        for result in hash_runner([p for _, p in need]):
            dst = back.get(result.get("path"))
            if dst is not None:
                record = {"dst": dst, "xxh128": result.get("xxh128"), "error": result.get("error")}
                _append(check_log, record)
                if record["xxh128"]:
                    checked[dst] = record

    rows, counts = [], Counter()
    for e in server_entries(plan):
        got = server_log.get(e["dst"], {}).get("xxh128")
        want = e.get("xxh128") or got
        status = "on master" if got and got == want else ("not copied" if e["dst"] not in server_log else "hash differs")
        counts[status] += 1
        if status != "on master":
            rows.append([status, "server", e["src"], e["dst"]])
    for e in pc_entries(plan):
        copied = pc_log.get(e["dst"])
        got = checked.get(e["dst"], {}).get("xxh128")
        want = e["xxh128"] or (copied or {}).get("xxh128")
        if copied is None:
            status = "not copied"
        elif not got:
            status = "not checked"
        else:
            status = "on master" if want and got == want else "hash differs"
        counts[status] += 1
        if status != "on master":
            rows.append([status, "pc", e["src"], e["dst"]])
    with (plan / "not_on_master.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["status", "copied from", "source", "destination"])
        writer.writerows(rows)
    result = {"checked_at": datetime.now().isoformat(timespec="seconds"), **counts,
              "complete": not rows}
    (plan / "master_check.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


# ---------------------------------------------------------------- the command


def summary(config: ScanConfig, plan: Path) -> tuple[int, int]:
    server = server_entries(plan)
    pc = pc_entries(plan)
    server_done = _done(plan / "server_copy_log.jsonl")
    pc_done = _done(plan / "pc_copy_log.jsonl")
    s_bytes = sum(e.get("size", 0) for e in server if e["dst"] not in server_done)
    p_bytes = sum(e["size"] for e in pc if e["dst"] not in pc_done)
    print(f"Build plan {plan.name}:")
    print(f"  server to server: {len(server):,} files, {len(server_done):,} already done, "
          f"{s_bytes / 1e9:,.1f} GB to copy")
    print(f"  PC to master:     {len(pc):,} files, {len(pc_done):,} already done, {p_bytes / 1e9:,.1f} GB to copy")
    return s_bytes, p_bytes


def run_copy_master(config_path, *, yes: bool = False, copy_runner: CopyRunner | None = None,
                    hash_runner: HashRunner | None = None, free_bytes: int | None = None) -> dict:
    config = load_config(config_path)
    plan = latest_build(config)
    master = master_root(config)
    if plan is None or master is None:
        raise SystemExit("Nothing to copy: run build-plan first, and give config.yml a root with role: master.")
    needed = sum(summary(config, plan))
    if free_bytes is None:
        disks = [d for d in _server_disks(config) if "master" in d[0]]
        free_bytes = disks[0][2] if disks else 0
    print(f"  the master has {free_bytes / 1e9:,.1f} GB free")
    if not yes:
        print("Nothing was copied. To copy, run the same command with --yes.")
        return {}
    if needed > free_bytes:
        raise SystemExit(f"The master has {free_bytes / 1e9:,.1f} GB free but {needed / 1e9:,.1f} GB is to be "
                         "copied. Nothing was copied.")
    started = time.monotonic()
    print("\n=== Step 1 of 3: server to server ===")
    server_counts = run_server_copies(plan, copy_runner or ssh_copy_runner(config))
    print("\n=== Step 2 of 3: PC to master ===")
    pc_counts = run_pc_copies(config, plan)
    print("\n=== Step 3 of 3: check every file on the master ===")
    result = check_master(config, plan, hash_runner or ssh_master_hash_runner(config, master))
    minutes = (time.monotonic() - started) / 60
    print(f"\nServer copies: {dict(server_counts)}; PC copies: {dict(pc_counts)} ({minutes:,.0f} minutes)")
    if result["complete"]:
        print("Every file in the plan is on the master with a matching hash.")
    else:
        print(f"Not yet complete: {result.get('not copied', 0):,} not copied, {result.get('hash differs', 0):,} "
              f"with a different hash, {result.get('not checked', 0):,} not checked. They are listed in "
              f"{plan / 'not_on_master.csv'}. Run copy-master --yes again to carry on.")
    return result
