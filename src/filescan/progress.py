from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TypeVar

from tqdm import tqdm

T = TypeVar("T")


def progress_bar(*, desc: str, total: int | None = None, unit: str = "item") -> tqdm:
    return tqdm(total=total, desc=desc, unit=unit, leave=True)


def track(iterable: Iterable[T], *, desc: str, total: int | None = None, unit: str = "item") -> Iterator[T]:
    yield from tqdm(iterable, desc=desc, total=total, unit=unit, leave=True)
