from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TypeVar

from tqdm import tqdm

T = TypeVar("T")


def progress_bar(*, desc: str, total: int | None = None, unit: str = "item", unit_scale: bool = False) -> tqdm:
    """unit_scale=True with unit="B" shows bytes as kB, MB, GB."""
    return tqdm(total=total, desc=desc, unit=unit, unit_scale=unit_scale, leave=True)


def track(iterable: Iterable[T], *, desc: str, total: int | None = None, unit: str = "item") -> Iterator[T]:
    yield from tqdm(iterable, desc=desc, total=total, unit=unit, leave=True)


def _reason(problem: object) -> str:
    """A short label to count problems by, such as 'PermissionError' or 'WinError 5'."""
    if isinstance(problem, BaseException):
        winerror = getattr(problem, "winerror", None)
        return f"{type(problem).__name__}" + (f" (WinError {winerror})" if winerror else "")
    return str(problem)


class SkipLog:
    """Problems with single files or folders, kept off the screen while a progress bar runs.

    Each one is written to <filescan folder>/logs/<name>-<time>.log and counted.
    The count shows beside the bar, and summary() prints the totals by reason
    once the bars are closed. Printing one line per file would break the bar
    and, with many thousands of files, slow the run down. Safe to call from
    worker threads. The log file is created only if something is logged.
    """

    def __init__(self, folder: Path | None, name: str) -> None:
        self.path = None if folder is None else Path(folder) / "logs" / f"{name}-{time.strftime('%Y%m%d-%H%M%S')}.log"
        self.counts: Counter[str] = Counter()
        self._handle = None
        self._lock = threading.Lock()
        self._bar: tqdm | None = None

    def attach(self, bar: tqdm) -> "SkipLog":
        """Show the running count beside this bar."""
        self._bar = bar
        return self

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    def add(self, what: str, path: object, problem: object) -> None:
        reason = _reason(problem)
        with self._lock:
            self.counts[f"{what}: {reason}"] += 1
            if self.path is not None:
                try:
                    if self._handle is None:
                        self.path.parent.mkdir(parents=True, exist_ok=True)
                        self._handle = self.path.open("a", encoding="utf-8")
                    self._handle.write(f"{what}\t{path}\t{problem}\n")
                except OSError:
                    self.path = None  # can't log to disk; the counts are still reported
            if self._bar is not None:
                self._bar.set_postfix_str(f"skipped {self.total:,}", refresh=False)

    def close(self) -> None:
        with self._lock:
            if self._handle is not None:
                self._handle.close()
                self._handle = None

    def summary(self) -> None:
        """Print the totals by reason; call it after the progress bars have closed."""
        self.close()
        if not self.counts:
            return
        print(f"Skipped {self.total:,}:")
        for reason, n in self.counts.most_common():
            print(f"  {n:>10,}  {reason}")
        if self.path is not None:
            print(f"  Each one is listed in {self.path}")
