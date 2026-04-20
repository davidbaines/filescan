# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`filescan` is a Windows file organisation tool using a **review-first** workflow:

```
scan -> duplicates -> similarity -> plan -> report -> execute (explicit only)
```

Nothing is deleted or moved without user approval. `execute` requires `--plan` and only acts on approved proposals.

## Commands

```bash
# Install dependencies
poetry install

# Run CLI
filescan --config config.yml run          # full non-destructive pipeline
filescan --config config.yml scan
filescan --config config.yml duplicates
filescan --config config.yml similarity
filescan --config config.yml plan
filescan --config config.yml report
filescan --config config.yml execute --plan /path/to/plan.json

# Run all tests
pytest

# Run a single test file or test
pytest tests/unit/test_config.py
pytest tests/unit/test_config.py::test_load_config_uses_paths_and_defaults

# Lint / format
flake8 src/
isort src/ tests/
black src/ tests/
```

## Architecture

**Entry point:** `src/filescan/cli.py` — parses subcommands, runs preflight DB checks, then dispatches to pipeline stages.

**Pipeline stages and their modules:**

| Stage | Module | Key class |
|-------|--------|-----------|
| Scan | `inventory/scanner.py` | `InventoryScanner` — multi-threaded recursive walk, writes `folders`/`files` to DB |
| Deduplicate | `dedupe/duplicates.py` | `DuplicateDetector` — 3-pass: size group → quick hash → full hash |
| Similarity | `similarity/folders.py` | `FolderSimilarityAnalyser` — weighted heuristics (file signatures 35%, shared duplicates 55%, name 10%) |
| Plan | `planning/proposals.py` | `ProposalBuilder` — emits `mark_backup`, `merge_folder`, `needs_review` actions |
| Report | `reporting/xlsx.py` | Custom XLSX writer (raw ZIP+XML, no external lib) |
| Execute | `execution/mover.py` | `ExecutionRunner` — copies trees, verifies via hash before writing |

**Data layer:** `storage/db.py` (`SQLiteDB`, WAL mode) + `storage/repositories.py` (`FileRepository`). The DB is the single source of truth for restartability — stages query what has already been processed and skip it unless `--rescan` is given.

**Models:** `models.py` defines all data classes (`FileRecord`, `FolderRecord`, `DuplicateGroup`, `FolderSimilarityCandidate`, `PlanProposal`, `ExecutionResult`).

**Hashing:** `dedupe/hashing.py` — quick hash reads 4 KB head + 4 KB tail + size; full hash streams the entire file in 1 MB chunks using xxhash128.

## Working Style

- Do not show code diffs in the terminal.

## Key Engineering Rules (from AGENTS.md)

- **Windows only** — use `pathlib.Path`, not `os` module string paths.
- **No auto-delete** — never remove source files in the normal pipeline.
- **Separate concerns** — exact duplicate detection and folder similarity are independent analyses.
- **Restartable** — pipeline progress is persisted in SQLite; a crash loses only the current batch.
- **Fail early** — preflight checks confirm the DB is writable before any work begins.
- **Error tolerance** — catch `PermissionError`/`OSError` on individual files, log and continue.
- **Progress visibility** — all long-running loops use `tqdm` with `leave=True`.
- **TDD** — tests live in `tests/unit/` (fast, isolated) and `tests/integration/` (full pipeline with temp file trees).

## Test Structure

- `tests/unit/` — isolated unit tests, no disk I/O beyond temp fixtures
- `tests/integration/` — full CLI pipeline tests using `tree_builder.build_tree()` to create synthetic file trees
- `tests/manual/report_fixture/` — persistent dataset for manual XLSX inspection
- `conftest.py` — provides `tmp_path` (Windows temp dir), `write_config`, and `make_repo` fixtures
