from __future__ import annotations

import shutil
from datetime import datetime

from filescan.models import ScanConfig
from filescan.storage import SQLiteDB

_BYTES_PER_FILE_ESTIMATE = 400
_AVG_FILE_SIZE_BYTES = 50_000  # 50 KB — conservative; drives with large media files will be over-estimated
_ABORT_RATIO = 0.90
_WARN_RATIO = 0.50


def check_db_space(config: ScanConfig) -> None:
    """Warn or abort if the estimated DB size would exhaust space on the database drive."""
    estimated_files, estimated_db_bytes = _estimate(config)
    db_dir = config.database_path.parent
    try:
        free = shutil.disk_usage(db_dir).free
    except OSError:
        return
    ratio = estimated_db_bytes / max(free, 1)
    if ratio >= _ABORT_RATIO:
        raise SystemExit(
            f"Insufficient disk space for database.\n"
            f"  DB path:              {config.database_path}\n"
            f"  Estimated DB size:    {_mb(estimated_db_bytes):,} MB"
            f"  (~{estimated_files:,} files × {_BYTES_PER_FILE_ESTIMATE} B/file)\n"
            f"  Free space on drive:  {_mb(free):,} MB\n"
            "Move the database to a drive with more free space, or reduce scan roots."
        )
    if ratio >= _WARN_RATIO:
        print(
            f"Warning: estimated DB size (~{_mb(estimated_db_bytes):,} MB) may use over half "
            f"of available space ({_mb(free):,} MB free on {db_dir})"
        )


def _estimate(config: ScanConfig) -> tuple[int, int]:
    total_files = 0
    for root in config.roots:
        try:
            used = shutil.disk_usage(root).used
            total_files += used // _AVG_FILE_SIZE_BYTES
        except OSError:
            pass
    return total_files, total_files * _BYTES_PER_FILE_ESTIMATE


def _mb(n: int) -> int:
    return n // (1024 * 1024)


def check_stale_roots(config: ScanConfig) -> None:
    """Warn if any root has not been scanned within config.scan_max_age."""
    if not config.database_path.exists():
        return
    try:
        db = SQLiteDB(config.database_path)
    except Exception:
        return
    try:
        now = datetime.now()
        cutoff = now - config.scan_max_age
        threshold_days = config.scan_max_age.days
        warnings: list[str] = []
        for root in config.roots:
            row = db.conn.execute(
                "SELECT last_scanned_at FROM scan_stats WHERE root_path = ?",
                (str(root),),
            ).fetchone()
            if row is None:
                warnings.append(f"  {root} — never scanned")
            else:
                last_scanned = datetime.fromisoformat(row["last_scanned_at"])
                if last_scanned < cutoff:
                    age_days = (now - last_scanned).days
                    warnings.append(f"  {root} — last scanned {age_days} days ago")
        if warnings:
            print(f"Warning: scan data is older than {threshold_days} days for:")
            for w in warnings:
                print(w)
            print("  Run 'filescan scan' to refresh.")
    finally:
        db.close()
