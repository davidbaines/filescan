"""Textual screen for reviewing a scope: one key or one click per decision.

Keys (also shown as clickable buttons):
  k keep    t trash    a accept the suggestion    A accept every suggestion (press twice)
  o open in Explorer    s split into subfolders    u undo    n next undecided
  h hide decided    / filter by path    T or K mark every undecided folder shown (press twice)
  q quit
"""

from __future__ import annotations

import subprocess
from pathlib import PureWindowsPath

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal, Vertical
from textual.widgets import Button, Footer, Header, Input, Label, ListItem, ListView, Static

from filescan.review.model import FILES, KEEP, TRASH, Unit, short_path
from filescan.review.session import ReviewSession


def gb(n: int) -> str:
    if n >= 1e9:
        return f"{n / 1e9:.1f} GB"
    if n >= 1e6:
        return f"{n / 1e6:.0f} MB"
    return f"{n / 1e3:.0f} KB"


_MARK = {KEEP: "[green]KEEP [/]", TRASH: "[red]TRASH[/]", None: "[dim]  ·  [/]"}


_LABEL_PREFIX = 35  # mark, two sizes and the suggestion hint


def unit_label(unit: Unit, width: int = 50) -> str:
    hint = ""
    if unit.decision is None and unit.suggestion:
        hint = "[yellow]→t[/] " if unit.suggestion.decision == TRASH else "[yellow]→k[/] "
    suffix = "\\(files here)" if unit.kind == FILES else ""
    path = short_path(unit.path, max(12, width - len(suffix))) + suffix
    return f"{_MARK[unit.decision]} {gb(unit.unique_bytes):>8} {gb(unit.total_bytes):>8}  {hint}{path}"


def unit_detail(unit: Unit) -> str:
    lines = [
        f"[b]{unit.path}[/b]" + ("  [dim](only the files directly in this folder)[/]" if unit.kind == FILES else ""),
        "",
        f"Unique: [b]{gb(unit.unique_bytes)}[/b]   Total: {gb(unit.total_bytes)}   "
        f"Files: {len(unit.files):,} ({sum(1 for f in unit.files if f.unique):,} unique)",
        f"Decision: {unit.decision or 'none yet'}",
    ]
    if unit.suggestion:
        lines.append(f"Suggestion: [yellow]{unit.suggestion.decision}[/] ({unit.suggestion.reason})")
    if unit.has_children and unit.kind != FILES:
        lines.append("[dim]Press s to split this folder into its subfolders.[/]")
    ext = unit.top_extensions()
    if ext:
        lines += ["", "[u]Unique data by type[/u]"]
        lines += [f"  {name:<10} {gb(size):>9}" for name, size in ext]
    largest = unit.largest_unique()
    if largest:
        lines += ["", "[u]Largest unique files[/u]"]
        for f in largest:
            rel = f.path[len(unit.path):].lstrip("\\") or PureWindowsPath(f.path).name
            lines.append(f"  {gb(f.size):>9}  {rel}")
    return "\n".join(lines)


class ReviewApp(App[None]):
    TITLE = "filescan review"
    BINDINGS = [
        Binding("k", "keep", "Keep"),
        Binding("t", "trash", "Trash"),
        Binding("a", "accept", "Accept suggestion"),
        Binding("A", "accept_all", "Accept all", show=False),
        Binding("o", "open", "Open in Explorer"),
        Binding("s", "split", "Split"),
        Binding("u", "undo", "Undo"),
        Binding("n", "next_undecided", "Next undecided"),
        Binding("h", "toggle_hide", "Hide decided"),
        Binding("slash", "filter", "Filter"),
        Binding("T", "trash_shown", "Trash all shown", show=False),
        Binding("K", "keep_shown", "Keep all shown", show=False),
        Binding("escape", "clear_filter", "Clear filter", show=False),
        Binding("q", "quit", "Quit"),
    ]
    DEFAULT_CSS = """
    #main { height: 1fr; }
    #left { width: 3fr; border-right: solid $accent; }
    #units { height: 1fr; }
    #columns { height: 1; color: $text-muted; }
    #side { width: 2fr; }
    #detail { height: 1fr; padding: 0 1; overflow-y: auto; }
    #buttons { height: 6; grid-size: 3 2; grid-gutter: 0 1; }
    #buttons Button { width: 100%; min-width: 6; }
    #summary { height: 1; background: $accent; color: $text; padding: 0 1; }
    #filter { display: none; }
    #filter.shown { display: block; }
    """

    def __init__(self, session: ReviewSession, *, open_command: list[str] | None = None) -> None:
        super().__init__()
        self.session = session
        self.open_command = open_command if open_command is not None else ["explorer.exe"]
        self.hide_decided = False
        self.shown: list[Unit] = []
        self._accept_all_armed = False
        self.filter_text = ""
        self._bulk_armed: str | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="main"):
            with Vertical(id="left"):
                yield Input(placeholder="Show folders whose path contains... (Enter to apply, Esc to clear)",
                            id="filter")
                yield Static("        unique    total     (→t: suggested trash)", id="columns")
                yield ListView(id="units")
            with Vertical(id="side"):
                yield Static(id="detail", markup=True)
                with Grid(id="buttons"):
                    yield Button("Keep (k)", id="b-keep", variant="success")
                    yield Button("Trash (t)", id="b-trash", variant="error")
                    yield Button("Accept (a)", id="b-accept", variant="warning")
                    yield Button("Open (o)", id="b-open", variant="primary")
                    yield Button("Split (s)", id="b-split")
                    yield Button("Undo (u)", id="b-undo")
        yield Static(id="summary", markup=True)
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = f"scope: {self.session.scope}. Decisions are recorded only; nothing is deleted here."
        self._rebuild(0)
        self.query_one("#units", ListView).focus()
        self.set_interval(5, self._save_pending)

    def _save_pending(self) -> None:
        if self.session.pending and self.session.save():
            self._refresh()

    # ------------------------------------------------------------ helpers

    def _rebuild(self, index: int = 0, select: Unit | None = None) -> None:
        needle = self.filter_text.lower()
        self.shown = [u for u in self.session.units
                      if not (self.hide_decided and u.decision) and needle in u.path.lower()]
        lv = self.query_one("#units", ListView)
        lv.clear()
        width = self._label_width()
        for unit in self.shown:
            lv.append(ListItem(Label(unit_label(unit, width), markup=True)))
        if select is not None and select in self.shown:
            index = self.shown.index(select)
        if self.shown:
            lv.index = max(0, min(index, len(self.shown) - 1))
        self._refresh()

    def _current(self) -> Unit | None:
        index = self.query_one("#units", ListView).index
        if index is None or not (0 <= index < len(self.shown)):
            return None
        return self.shown[index]

    def _refresh(self) -> None:
        unit = self._current()
        self.query_one("#detail", Static).update(unit_detail(unit) if unit else "Nothing to review.")
        t = self.session.totals()
        shown = f"   [b]filter: {self.filter_text!r}, {len(self.shown):,} shown[/]" if self.filter_text else ""
        self.query_one("#summary", Static).update(
            f"{t['decided']:,}/{t['units']:,} decided{shown}   unique: {gb(t['unique'])}   "
            f"keep {gb(t['keep'])}   trash {gb(t['trash'])}   undecided {gb(t['undecided'])}"
            + (f"   [b]{len(self.session.pending)} waiting for the database (safe in the journal)[/]"
               if self.session.pending else "")
        )

    def _label_width(self) -> int:
        lv = self.query_one("#units", ListView)
        available = lv.size.width or (self.size.width * 3) // 5
        return max(20, available - _LABEL_PREFIX)

    def _relabel(self, only: Unit | None = None) -> None:
        lv = self.query_one("#units", ListView)
        width = self._label_width()
        if only is not None:
            if only in self.shown:
                item = lv.children[self.shown.index(only)]
                item.query_one(Label).update(unit_label(only, width))
            return
        for item, unit in zip(lv.children, self.shown):
            item.query_one(Label).update(unit_label(unit, width))

    def on_resize(self, _event) -> None:
        self.call_after_refresh(self._relabel)

    def _advance(self) -> None:
        lv = self.query_one("#units", ListView)
        start = (lv.index or 0) + 1
        for i in range(start, len(self.shown)):
            if self.shown[i].decision is None:
                lv.index = i
                return
        if start < len(self.shown):
            lv.index = start

    def _decide(self, decision: str) -> None:
        unit = self._current()
        if unit is None:
            return
        others_changed = self.session.decide(unit, decision)
        if self.hide_decided:
            self._rebuild(self.query_one("#units", ListView).index or 0)
        else:
            self._relabel(None if others_changed else unit)
            self._advance()
        self._refresh()

    # ------------------------------------------------------------ events

    def on_list_view_highlighted(self, _event: ListView.Highlighted) -> None:
        self._refresh()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        actions = {"b-keep": self.action_keep, "b-trash": self.action_trash, "b-accept": self.action_accept,
                   "b-open": self.action_open, "b-split": self.action_split, "b-undo": self.action_undo}
        action = actions.get(event.button.id or "")
        if action:
            action()

    # ------------------------------------------------------------ actions

    def action_keep(self) -> None:
        self._decide(KEEP)

    def action_trash(self) -> None:
        self._decide(TRASH)

    def action_accept(self) -> None:
        unit = self._current()
        if unit is None or unit.suggestion is None:
            self.notify("No suggestion for this folder.")
            return
        self._decide(unit.suggestion.decision)

    def action_accept_all(self) -> None:
        todo = [u for u in self.session.units if u.decision is None and u.suggestion is not None]
        if not todo:
            self.notify("No undecided folders have a suggestion.")
            return
        if not self._accept_all_armed:
            self._accept_all_armed = True
            self.notify(f"Press A again to accept the suggestion for {len(todo)} folders.")
            return
        self._accept_all_armed = False
        self.session.decide_many(todo, [u.suggestion.decision for u in todo])
        self._rebuild(self.query_one("#units", ListView).index or 0)
        self.notify(f"Accepted {len(todo)} suggestions. Press u on any of them to undo it.")

    def action_undo(self) -> None:
        unit = self._current()
        if unit is None or unit.decision is None:
            return
        others_changed = self.session.clear(unit)
        self._relabel(None if others_changed else unit)
        self._refresh()

    def action_open(self) -> None:
        unit = self._current()
        if unit is None:
            return
        try:
            # explorer.exe returns 1 even when it succeeds, so the exit code is ignored.
            subprocess.Popen([*self.open_command, unit.path])
        except OSError as exc:
            self.notify(f"Could not open {unit.path}: {exc}", severity="error")

    def action_split(self) -> None:
        unit = self._current()
        if unit is None:
            return
        if unit.decision is not None:
            self.notify("Undo the decision before splitting this folder.")
            return
        new_units = self.session.split(unit)
        if not new_units:
            self.notify("This folder has no subfolders to split into.")
            return
        first = min(new_units, key=lambda u: self.session.units.index(u))
        self._rebuild(select=first)

    def action_next_undecided(self) -> None:
        self._advance()
        self._refresh()

    def action_toggle_hide(self) -> None:
        self.hide_decided = not self.hide_decided
        self._rebuild(select=self._current())

    def action_filter(self) -> None:
        box = self.query_one("#filter", Input)
        box.add_class("shown")
        box.value = self.filter_text
        box.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self._apply_filter(event.value.strip())

    def _apply_filter(self, text: str) -> None:
        box = self.query_one("#filter", Input)
        self.filter_text = text
        self._bulk_armed = None
        if not text:
            box.remove_class("shown")
        self._rebuild(select=self._current())
        self.query_one("#units", ListView).focus()

    def action_clear_filter(self) -> None:
        self._apply_filter("")

    def _mark_shown(self, decision: str) -> None:
        todo = [u for u in self.shown if u.decision is None]
        if not todo:
            self.notify("Every folder shown already has a decision.")
            return
        word, key = ("trash", "T") if decision == TRASH else ("keep", "K")
        if self._bulk_armed != decision:
            self._bulk_armed = decision
            size = gb(sum(u.total_bytes for u in todo))
            self.notify(f"Press {key} again to mark the {len(todo):,} undecided folders shown ({size}) as {word}.")
            return
        self._bulk_armed = None
        self.session.decide_many(todo, [decision] * len(todo))
        self._rebuild(select=self._current())
        self.notify(f"Marked {len(todo):,} folders as {word}. Press u on any of them to undo it.")

    def action_trash_shown(self) -> None:
        self._mark_shown(TRASH)

    def action_keep_shown(self) -> None:
        self._mark_shown(KEEP)


def run_review(config_path, scope: str | None, *, split_gb: float = 5.0) -> None:
    from filescan.config import load_config
    from filescan.storage import SQLiteDB

    config = load_config(config_path)
    if not scope:
        names = [r.name for r in config.exclusions if r.disposition == "delete-candidate"]
        print("Choose a scope to review: " + ", ".join(names))
        return
    db = SQLiteDB(config.database_path)
    try:
        session = ReviewSession(db, config, scope, split_gb=split_gb)
        session.load()
        ReviewApp(session).run()
        out = session.export_csv()
        t = session.totals()
        if session.pending and not session.save(wait_ms=30_000):
            print(f"The database is busy, so {len(session.pending):,} decisions wait in {session.journal_path}; "
                  "they are saved to the database the next time you run the review.")
        print(f"{t['decided']:,} of {t['units']:,} folders decided. Decisions are also in {out}")
    finally:
        db.close()
