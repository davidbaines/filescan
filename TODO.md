## To Do

- [x] 1. `large_file_size` config parameter — add to config.yml / ScanConfig, used by largest file finder
- [x] 2. Largest file finder — new `largest` CLI command, Textual TUI output
- [x] 3. AGENTS.md compliance audit — find and fix all rule violations in the codebase (run after Tasks 1 & 2 so new code is included)
- [x] 4. Fix TypeError in `refresh.py` — `track()` called with unsupported `leave=False` kwarg
- [x] 5. Duplicate folder merge — backend: clustering, hierarchy suppression, unique-file identification
- [ ] 6. Duplicate folder merge — plan artifact extension (cluster-aware merge proposals in existing JSON schema)
- [ ] 7. Duplicate folder merge — Textual TUI review (`filescan merge-review` command)
- [ ] 8. Duplicate folder merge — execute pipeline update (cluster merges via existing ExecutionRunner)
- [ ] 9. Implement a waste_file_size threshold so that small files are not reported.

---

## Implementation Plans

### 1. `large_file_size` Config Parameter

Switch `roots` to object format in config.yml (Option C — breaking change, no released version):
```yaml
roots:
  - path: C:/
  - path: D:/
    large_file_size: 2GB
  - path: E:/
```

Add `_parse_size(value) -> int` helper to `config.py`:
- Accepts int (bytes), or string with optional space + unit: B, KB, MB, GB (case-insensitive, 1024-based)
- e.g. `"500MB"`, `"500 MB"`, `"1.5GB"`, `500000000` all valid

Add to `ScanConfig` dataclass (`models.py`):
```python
large_file_size: int = 500_000_000          # global default (500 MB)
large_file_thresholds: dict[Path, int] = field(default_factory=dict)  # per-root overrides
```

Add helper method to `ScanConfig`:
```python
def large_file_size_for(self, root: Path) -> int:
    return self.large_file_thresholds.get(root, self.large_file_size)
```

Update `config.py` loader to:
- Parse each root item as a dict with required `path` key and optional `large_file_size`
- Parse top-level `large_file_size` with `_parse_size`
- Build `large_file_thresholds` dict from roots that specify their own threshold

Update `C:\filescan\config.yml` to use the new root object format.

Fix any test fixtures that use the old `roots: [string]` format.

Write unit tests in `tests/unit/test_config.py`:
- `_parse_size` with bytes, KB, MB, GB, float values, case variants
- Config load with global threshold only
- Config load with per-root override + fallback to global

---

### 2. Largest File Finder — Textual TUI

New module: `src/filescan/reporting/largest_files.py`

**Data query:**
1. Open the scanned DB (no filesystem walk — fast)
2. For each root in config, query files where `path LIKE root%` and `size >= config.large_file_size_for(root)`
3. Merge results, sort by size descending

**Textual TUI (`poetry add textual`):**
- Table columns: Rank, Size, Filename, Full Path
- Keyboard:
  - Enter → `subprocess.Popen(['explorer.exe', str(file.parent)])` opens containing folder
  - Space → toggle mark on row; footer shows count + total bytes of marked files
  - Q / Escape → quit
- Filter bar (optional, v2): type to filter by drive or filename substring

**CLI:**
- New subcommand: `filescan --config config.yml largest`
- Add parser entry in `build_parser()` in `cli.py`
- Dispatch in `main()`
- Requires a prior `scan` run (fails with clear message if DB has no files)

**AGENTS.md rules to follow in new code:**
- Use `pathlib.Path` throughout, no `os` string paths
- Catch `PermissionError`/`OSError` per file, log and continue
- No auto-delete

---

### 3. AGENTS.md Compliance Audit

Read every rule in AGENTS.md and check each source file against it. Produce a numbered list of violations, then fix each one after user review.

Rules to check across all modules:
- All long-running loops use `tqdm` with `leave=True`
- `pathlib.Path` used instead of `os` module string paths
- No auto-delete anywhere in the normal pipeline
- Duplicate detection and folder similarity are fully independent (no cross-calls)
- Startup checks are fast and fail early (DB writability before any work)
- Code is restartable (DB tracks stage progress, `--rescan` flag honoured everywhere)
- `PermissionError`/`OSError` caught on individual files, log and continue
- Default pipeline is non-destructive

Deliverable: numbered list of findings for user review, then fixes.

---

### 4. Fix TypeError in `refresh.py`

`track()` in `progress.py` does not accept a `leave` keyword argument, but
`refresh_file_records()` calls `track(..., leave=False)`.

Fix: remove the `leave=False` argument from the `track()` call in
`src/filescan/inventory/refresh.py`.

---

### 5–8. Duplicate Folder Merge Feature

**Architecture decision:** extend the existing `plan`/`execute` pipeline (Option A).
Cluster data goes into the existing plan artifact JSON; the TUI writes to it;
`ExecutionRunner` reads from it. No parallel command infrastructure needed.

**UI decision:** Textual TUI for interactive review + YAML/JSON plan artifact
that is also directly editable for scripted or headless workflows.

---

#### 5. Backend: Clustering, Hierarchy Suppression, Unique-File Identification

**Module:** `src/filescan/similarity/clusters.py`

**Clustering (union-find):**
- Input: all `folder_similarity_candidates` rows from DB with score ≥ threshold
- Add `similarity_cluster_threshold: float = 0.70` to `ScanConfig` (config key
  `similarity_cluster_threshold`)
- Union-find groups connected pairs into clusters; each cluster is a list of 2+
  folder paths with their pairwise scores
- Store clusters in DB: new tables `similarity_clusters` (id, min_score,
  created_at) and `similarity_cluster_members` (cluster_id, folder_id)

**Hierarchy suppression:**
- Sort clusters by the minimum depth of their members (shallowest first)
- For each cluster, check whether every member folder has an ancestor already
  present in an earlier (shallower) cluster
- If so, mark the cluster `is_suppressed = 1` in `similarity_clusters`
- Suppressed clusters don't appear in the TUI by default; user can show them
  with a toggle

**Unique-file identification:**
- For each cluster, choose an initial master using heuristics (in priority order):
  1. Folder is inside a preferred root (deepest configured root that contains it)
  2. Highest file count
  3. Shallowest depth in the filesystem tree
  4. Most recent mtime
- For each non-master folder F in the cluster, query: files under F whose hash
  does not appear in any `duplicate_group_members` entry pointing to a file
  under the master. These are "unique to F" and must be copied to master before
  F can be safely deleted.
- Store in DB: new table `cluster_unique_files` (cluster_id, file_id,
  source_folder_id) for fast lookup

**New DB migrations:** add tables above to `storage/db.py` schema.

**Entry point:** `build_clusters(config_path)` → returns list of `FolderCluster`
dataclass objects. Add to `ProposalBuilder` so `plan` stage calls it.

**Tests:** `tests/unit/test_similarity_clusters.py`
- union-find produces correct clusters from pairs
- hierarchy suppression suppresses child clusters correctly
- unique-file query returns correct set when hashes are present/absent

---

#### 6. Plan Artifact Extension

**Module:** `src/filescan/planning/proposals.py` (extend existing)

Add `merge_cluster` proposal type alongside existing `merge_folder`:
```json
{
  "action": "merge_cluster",
  "cluster_id": 1,
  "score": 0.94,
  "status": "pending",
  "master": "F:\\Work\\Reports",
  "copies": [
    {
      "path": "G:\\Backup\\Reports",
      "unique_file_count": 0,
      "unique_files": []
    }
  ]
}
```

`status` field: `"pending"` | `"approved"` | `"rejected"` | `"skipped"`

The TUI reads and writes this field. `ExecutionRunner` only acts on `"approved"`
proposals. YAML and JSON are both acceptable plan formats; keep JSON as primary
since `load_plan_artifact` already parses it.

---

#### 7. Textual TUI — Merge Review

**Module:** `src/filescan/similarity/merge_review.py`

**Command:** `filescan merge-review` (new subparser in `cli.py`; also callable
after `filescan plan` via `filescan merge-review --plan path/to/plan.json`)

**Layout:**
```
┌─ Folder Merge Review ────────────────────────────────────────────────────────┐
│ 5 clusters · 0 approved · 0 rejected · 5 pending                             │
├────────────────────────────────┬─────────────────────────────────────────────┤
│ Clusters                       │ Cluster Detail                              │
│ ► [0.94] Reports        2.3GB  │ Score: 0.94 · 130 shared files              │
│   [0.87] Archive 2023   800MB  │                                             │
│   [0.91] Photos Backup  4.1GB  │ ★ MASTER  F:\Work\Reports                  │
│   [0.88] Invoices       220MB  │   130 files · 2.3 GB                       │
│   [0.72] Old Projects   1.2GB  │   3 files unique to master                 │
│                                │                                             │
│                                │   COPY    G:\Backup\Reports                │
│                                │   127 files · 2.2 GB                       │
│                                │   0 files unique to copy                   │
│                                │                                             │
│                                │ Unique files in master:                    │
│                                │   report_final_v3.docx      2.1 MB        │
│                                │   budget_2024.xlsx         41.0 MB        │
│                                │   notes_confidential.txt    2 KB          │
└────────────────────────────────┴─────────────────────────────────────────────┘
  a:approve  r:reject  s:skip  m:cycle master  o:open in Explorer  q:save & quit
```

**Keyboard bindings:**
- `a` — approve selected cluster (status → `"approved"`)
- `r` — reject selected cluster (status → `"rejected"`)
- `s` — skip / defer selected cluster (status → `"skipped"`)
- `m` — cycle master selection through cluster members
- `o` — open selected folder in Windows Explorer
- `h` — toggle display of suppressed (child) clusters
- `q` / `Escape` — save plan artifact and quit

**On quit:** write the updated plan JSON (with status fields updated) back to
the same path the plan was loaded from, or to a new path if `--out` was given.

**Tests:** `tests/unit/test_merge_review.py` — keyboard actions update plan
status correctly (no disk I/O, monkeypatch the TUI app).

---

#### 8. Execute Pipeline Update

**Module:** `src/filescan/execution/mover.py` (extend existing)

`ExecutionRunner` already handles `merge_folder` proposals with hash-verified
copy. Extend to handle `merge_cluster`:

For each `merge_cluster` proposal with `status == "approved"`:
1. For each copy folder's `unique_files` list:
   - Copy file to master using the existing hash-verified copy logic
   - Verify hash of copy matches source before proceeding
   - On failure: log and continue (do not abort entire cluster)
2. After all unique files are copied, mark the copy folder as a candidate for
   deletion — **do not delete automatically**. Instead:
   - Print a summary: "G:\Backup\Reports is now fully merged into master. Safe
     to delete."
   - Optionally write a `.filescan-merged` marker file in the copy folder so
     the user can identify it later
3. Update `cluster_unique_files` in DB to reflect completed copies

**No auto-delete** — consistent with AGENTS.md rule. The user deletes the copy
folder manually or via a separate `filescan cleanup` command (future work).

---

### 9. `waste_file_size` Config Threshold

Suppress waste candidates whose total size is below the configured threshold,
so trivially small cache/temp folders don't clutter the report.

**Config key:** `waste_file_size` (top-level, same level as `large_file_size`).
Parsed by the existing `_parse_size()` helper — same syntax (`200MB`, `1GB`,
raw int bytes). Default: `0` (no suppression — all candidates reported).

The user has added `waste_file_size: 200MB` to `C:\filescan\config.yml`.

**Changes required:**

`src/filescan/models.py`
- Add `waste_file_size: int = 0` to `ScanConfig` alongside `large_file_size`.

`src/filescan/config.py`
- Parse `data.get("waste_file_size")` with `_parse_size()` (same pattern as
  `large_file_size`); fall back to `0` if absent.
- Pass `waste_file_size=waste_file_size` in the `ScanConfig(...)` constructor call.

`src/filescan/cleanup/waste_detector.py`
- `find_waste_candidates` already loads config from `config_path`.
- After collecting and refreshing candidates, apply the filter just before
  returning:
  ```python
  candidates = [c for c in candidates if c.size_bytes >= config.waste_file_size]
  ```
- The filter applies to the *total aggregate size* of the candidate folder
  (all files recursively, as computed by `_recursive_stats`). Individual file
  sizes are not filtered — only the folder total determines inclusion.

`tests/unit/test_config.py`
- `test_load_config_reads_waste_file_size`: config with `waste_file_size: 500MB`
  → `config.waste_file_size == 500 * 1024 * 1024`.
- `test_load_config_waste_file_size_defaults_to_zero`: config without the key
  → `config.waste_file_size == 0`.

`tests/unit/test_waste_detector.py` (new file or extend existing)
- `test_find_waste_candidates_filters_small_candidates`: two candidates, one
  below threshold, one above; verify only the larger one is returned.

---

## Completed

1. `large_file_size` config parameter — `_parse_size()` helper, roots now object format in config.yml, `large_file_size` / `large_file_thresholds` / `large_file_size_for()` added to `ScanConfig`
2. Largest file finder — `src/filescan/reporting/largest_files.py` (Textual TUI), `filescan largest` CLI command
3. AGENTS.md compliance audit — fixed: `os.scandir` → `Path.iterdir()` in scanner.py; tqdm added to `find_waste_candidates`; exception logging added in `largest_files.py`
4. TypeError fix — removed `leave=False` from `track()` call in `refresh.py` (unsupported kwarg)
5. Duplicate folder merge — backend clustering: `src/filescan/similarity/clusters.py` (new). Union-find groups similarity pairs above `similarity_cluster_threshold` (default 0.70) into `FolderCluster` objects. Three-track dispatch: mark_backup pairs excluded, remaining pairs above threshold → clusters, pairs below threshold remain as needs_review. Hierarchy suppression: child cluster suppressed only when every member has an ancestor in one single parent cluster. Master selected by preferred root → highest file count → shallowest depth → newest mtime. Unique files identified by full_hash comparison against master; unhashed files treated conservatively as unique. `recompute_unique_files()` helper for TUI master-cycling. `filescan clusters [--show-suppressed]` debug command dumps clusters as JSON. `ClusterMember` and `FolderCluster` dataclasses added to `models.py`; `similarity_cluster_threshold` added to `ScanConfig` and `config.py`.
