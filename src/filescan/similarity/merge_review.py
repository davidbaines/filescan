from __future__ import annotations

import json
import subprocess
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Label, ListItem, ListView, Static

from filescan.config import load_config
from filescan.planning.artifacts import latest_plan_artifact, load_plan_artifact
from filescan.similarity.clusters import _cycle_master_in_dict
from filescan.storage import SQLiteDB


def _fmt_size(b: int) -> str:
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.1f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.1f} MB"
    return f"{b / 1024:.1f} KB"


_STATUS_ICON: dict[str, str] = {
    "pending": "·",
    "approved": "✓",
    "rejected": "✗",
    "skipped": "~",
}


def _list_label(cluster: dict) -> str:
    master = next((m for m in cluster["members"] if m["is_master"]), cluster["members"][0])
    total_bytes = sum(m["total_bytes"] for m in cluster["members"])
    icon = _STATUS_ICON.get(cluster["status"], "·")
    name = Path(master["path"]).name[:22]
    return f"{icon} [{cluster['min_score']:.2f}] {name:<22} {_fmt_size(total_bytes):>8}"


def _detail_text(cluster: dict) -> str:
    lines: list[str] = [f"Score: {cluster['min_score']:.4f}"]
    if cluster["is_suppressed"]:
        lines.append("[dim](hierarchy-suppressed — child of a larger cluster)[/dim]")
    lines.append("")

    for member in cluster["members"]:
        role = "[bold]★ MASTER[/bold]" if member["is_master"] else "  COPY  "
        lines.append(f"{role}  {member['path']}")
        lines.append(
            f"         {member['file_count']:,} files  ·  {_fmt_size(member['total_bytes'])}"
            f"  ·  {member['unique_file_count']} unique to this folder"
        )
        lines.append("")

    for member in cluster["members"]:
        if not member["is_master"] and member["unique_files"]:
            lines.append(f"[underline]Unique files in {Path(member['path']).name}:[/underline]")
            for p in member["unique_files"][:15]:
                lines.append(f"  {Path(p).name}")
            if len(member["unique_files"]) > 15:
                lines.append(f"  … and {len(member['unique_files']) - 15} more")
            lines.append("")

    return "\n".join(lines)


class MergeReviewApp(App[None]):
    TITLE = "Folder Merge Review"
    BINDINGS = [
        Binding("a", "approve", "Approve"),
        Binding("r", "reject", "Reject"),
        Binding("s", "skip", "Skip"),
        Binding("m", "cycle_master", "Cycle master"),
        Binding("o", "open_explorer", "Open folder"),
        Binding("h", "toggle_suppressed", "Show suppressed"),
        Binding("q", "quit_save", "Save & quit"),
        Binding("escape", "quit_save", "Save & quit"),
    ]

    DEFAULT_CSS = """
    Horizontal { height: 1fr; }
    #cluster-list { width: 42; border-right: solid $accent; }
    #cluster-detail { width: 1fr; padding: 1 2; overflow-y: auto; }
    #summary { height: 1; background: $accent; color: $text; padding: 0 1; }
    """

    def __init__(self, plan_path: Path, db: SQLiteDB) -> None:
        super().__init__()
        self._plan_path = plan_path
        self._db = db
        self._all_clusters: list[dict] = []
        self._show_suppressed = False

    @property
    def _visible(self) -> list[dict]:
        if self._show_suppressed:
            return self._all_clusters
        return [c for c in self._all_clusters if not c["is_suppressed"]]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            yield ListView(id="cluster-list")
            yield Static(id="cluster-detail", markup=True)
        yield Static(id="summary")
        yield Footer()

    def on_mount(self) -> None:
        payload = load_plan_artifact(self._plan_path)
        self._all_clusters = payload.get("clusters", [])
        self._rebuild_list()
        self._refresh_summary()

    def _rebuild_list(self, restore_index: int | None = None) -> None:
        lv = self.query_one("#cluster-list", ListView)
        lv.clear()
        for cluster in self._visible:
            lv.append(ListItem(Label(_list_label(cluster))))
        if restore_index is not None and self._visible:
            lv.index = min(restore_index, len(self._visible) - 1)
        self._refresh_detail()

    def _current_index(self) -> int | None:
        return self.query_one("#cluster-list", ListView).index

    def _current_cluster(self) -> dict | None:
        idx = self._current_index()
        visible = self._visible
        if idx is None or not (0 <= idx < len(visible)):
            return None
        return visible[idx]

    def _refresh_detail(self) -> None:
        cluster = self._current_cluster()
        detail = self.query_one("#cluster-detail", Static)
        if cluster is None:
            detail.update("No clusters to display.")
            return
        detail.update(_detail_text(cluster))

    def _refresh_summary(self) -> None:
        visible = self._visible
        approved = sum(1 for c in visible if c["status"] == "approved")
        rejected = sum(1 for c in visible if c["status"] == "rejected")
        skipped = sum(1 for c in visible if c["status"] == "skipped")
        pending = sum(1 for c in visible if c["status"] == "pending")
        suppressed = sum(1 for c in self._all_clusters if c["is_suppressed"])
        msg = (
            f"{len(visible)} clusters · {approved} approved · {rejected} rejected"
            f" · {skipped} skipped · {pending} pending"
        )
        if suppressed and not self._show_suppressed:
            msg += f"  (h: show {suppressed} suppressed)"
        self.query_one("#summary", Static).update(msg)

    def on_list_view_highlighted(self, _event: ListView.Highlighted) -> None:
        self._refresh_detail()

    def _set_current_status(self, status: str) -> None:
        cluster = self._current_cluster()
        if cluster is None:
            return
        idx = self._current_index()
        cluster["status"] = status
        self._rebuild_list(restore_index=idx)
        self._refresh_summary()

    def action_approve(self) -> None:
        self._set_current_status("approved")

    def action_reject(self) -> None:
        self._set_current_status("rejected")

    def action_skip(self) -> None:
        self._set_current_status("skipped")

    def action_cycle_master(self) -> None:
        cluster = self._current_cluster()
        if cluster is None or len(cluster["members"]) < 2:
            return
        idx = self._current_index()
        _cycle_master_in_dict(cluster, self._db)
        self._rebuild_list(restore_index=idx)

    def action_open_explorer(self) -> None:
        cluster = self._current_cluster()
        if cluster is None:
            return
        master = next((m for m in cluster["members"] if m["is_master"]), cluster["members"][0])
        try:
            subprocess.Popen(["explorer.exe", master["path"]])
        except OSError:
            pass

    def action_toggle_suppressed(self) -> None:
        idx = self._current_index()
        self._show_suppressed = not self._show_suppressed
        self._rebuild_list(restore_index=idx)
        self._refresh_summary()

    def action_quit_save(self) -> None:
        payload = load_plan_artifact(self._plan_path)
        payload["clusters"] = self._all_clusters
        self._plan_path.write_text(json.dumps(payload, indent=2))
        self.exit()

    def on_unmount(self) -> None:
        self._db.close()


def run_merge_review(config_path: Path, plan_path: Path | None = None) -> None:
    config = load_config(config_path)

    if plan_path is None:
        plan_path = latest_plan_artifact(config.artifact_dir)
        if plan_path is None:
            print("No plan artifact found. Run 'filescan plan' first.")
            return

    payload = load_plan_artifact(plan_path)
    clusters = payload.get("clusters", [])
    if not clusters:
        print("Plan contains no folder clusters. Run 'filescan plan' first or check similarity results.")
        return

    db = SQLiteDB(config.database_path)
    visible = sum(1 for c in clusters if not c["is_suppressed"])
    print(f"Opening merge review: {visible} cluster(s) to review ({len(clusters)} total including suppressed).")
    MergeReviewApp(plan_path, db).run()
