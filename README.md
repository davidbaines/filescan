# filescan

A Windows file organisation tool built around a **review-first** workflow — nothing is moved or deleted without explicit user approval.

## What it does

`filescan` scans one or more folder trees, identifies exact duplicate files and similar folder groups, proposes what to consolidate, and lets you approve each action before anything touches the filesystem. The `execute` command only runs on proposals you have approved.

## Pipeline

```
scan → duplicates → similarity → plan → report → execute (explicit only)
```

| Stage | Command | What it does |
|-------|---------|--------------|
| Scan | `filescan scan` | Recursive walk; records files and folders in SQLite |
| Duplicates | `filescan duplicates` | Groups exact duplicate files by content hash |
| Similarity | `filescan similarity` | Scores folder pairs by shared content |
| Plan | `filescan plan` | Writes a JSON plan artifact with proposals and clusters |
| Report | `filescan report` | Writes an XLSX summary report |
| Execute | `filescan execute --plan <path>` | Copies unique files for approved proposals and clusters |

**Duplicates vs similar folders** — `duplicates` finds files that are byte-for-byte identical (same content hash). `similarity` uses those duplicates to score folder pairs: two folders that share most of their files score highly. `plan` then groups high-scoring pairs into *clusters* — sets of folders that collectively contain the same content. The user-facing entry point for reviewing and acting on folder redundancy is `filescan merge-review`, not the individual pipeline commands.

### Interactive review commands

| Command | What it does |
|---------|--------------|
| `filescan largest` | Textual TUI: browse and mark large files for recycle |
| `filescan waste` | List wasteable folders (caches, temp, old Windows installs) |
| `filescan merge-review` | Textual TUI: approve/reject/skip folder merge clusters |
| `filescan clusters` | Debug: dump current clusters as JSON |

## Quick start

```bash
# Install
poetry install

# Full non-destructive pipeline
filescan --config config.yml run

# Individual stages
filescan --config config.yml scan
filescan --config config.yml duplicates
filescan --config config.yml similarity
filescan --config config.yml plan

# Interactive review
filescan --config config.yml merge-review
filescan --config config.yml largest
filescan --config config.yml waste

# Execute approved proposals (requires --plan)
filescan --config config.yml execute --plan path/to/plan-<id>.json
```

## Config file

```yaml
roots:
  - path: C:/Users/David/Documents
  - path: D:/Backup
    large_file_size: 2GB        # per-root override

filescan_folder: C:/filescan
database_folder: C:/filescan
database_filename: file_index.db
report_filename:  filescan_report.xlsx

# Thresholds
large_file_size:              500MB   # files above this appear in 'largest'
waste_file_size:              200MB   # waste folders below this are hidden
duplicate_size_threshold:     1GB     # minimum size for duplicate groups
similarity_threshold:         0.8     # minimum score to store a similarity pair
similarity_cluster_threshold: 0.70    # minimum score to form a merge cluster
merge_threshold:              0.93    # score above which a pair is a backup candidate
scan_max_age:                 30d     # warn if scan is older than this

# Optional filters
exclude_folders: [.git, __pycache__, node_modules]
exclude_extensions: [.tmp, .log]
worker_count: 4
```

## Design principles

- **Review-first** — `execute` only acts on proposals with `approval_state: approved` or cluster `status: approved`. Nothing is moved or deleted by default.
- **No auto-delete** — source folders are never removed. A `.filescan-merged` marker file is written to successfully merged copy folders; the user deletes them manually.
- **Restartable** — pipeline progress is stored in SQLite. A crash loses only the current batch; re-running a stage picks up where it left off (use `--rescan` to force a full re-run).
- **Windows-only** — paths use `pathlib.Path` with Windows separators throughout.
- **Error-tolerant** — `PermissionError` and `OSError` on individual files are logged and skipped; the pipeline continues.

## Architecture

See `CLAUDE.md` for module-level architecture notes, and `AGENTS.md` for the engineering rules applied throughout the codebase.

## Development

```bash
poetry install
pytest                          # full test suite
pytest tests/unit/              # fast unit tests only
flake8 src/
black src/ tests/
isort src/ tests/
```
