from __future__ import annotations

import re
from pathlib import Path

from filescan.cli import COMMAND_GROUPS, HELP, PLANNED_GROUPS

README = Path(__file__).resolve().parents[2] / "README.md"
PLANNED = {name for _, items in PLANNED_GROUPS for name, _ in items}
RETIRED = {"run", "similarity", "plan", "merge-review", "execute", "report", "waste", "largest", "clusters",
           "duplicates", "hash-server"}


def _named_in_readme() -> set[str]:
    return set(re.findall(r"`filescan ([a-z][a-z-]*)", README.read_text(encoding="utf-8")))


def test_every_command_the_readme_names_exists_or_is_planned():
    unknown = _named_in_readme() - set(HELP) - PLANNED
    assert not unknown, f"README.md names commands that do not exist: {sorted(unknown)}"


def test_the_readme_describes_every_command_in_the_order_of_the_help():
    text = README.read_text(encoding="utf-8")
    # The last group holds the single steps of scan, which the README leaves out.
    order = [name for _, items in COMMAND_GROUPS[:-1] for name, _ in items if name != "status"]
    positions = [text.find(f"| `filescan {name}") for name in order]
    assert all(p >= 0 for p in positions), [n for n, p in zip(order, positions) if p < 0]
    assert positions == sorted(positions), "README.md lists the commands in a different order from the help"


def test_retired_commands_are_not_offered():
    assert not RETIRED & set(HELP)
