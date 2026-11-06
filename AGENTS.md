# AGENTS.md

Guidance for coding agents working in this repository. `CLAUDE.md` points here, and this file is the single source of project rules.

## Project intent

`filescan` organises files on Windows with a review-first workflow:

```
scan -> duplicates -> similarity -> plan -> report -> execute (explicit only)
```

Nothing is deleted or moved without user approval. `execute` requires `--plan` and acts only on approved proposals.

The current work extends filescan to compare the PC with a Linux home server and to build one organised set of files. That work happens on the `server_mirror` branch, and [PLAN.md](PLAN.md) describes it, with checklists recording progress. Read PLAN.md before starting work on that branch and tick items off as they are completed.

## Commands

```bash
poetry install                                   # install dependencies

# --config can be left out when FILESCAN_CONFIG is set in the environment or in .env at the
# repository root (copy .env.example). There is no built-in default. With no command,
# filescan prints the help, grouped by stage.
filescan status                                  # where the work stands and what to run next (reads only)
filescan delete-folder "D:\Old" [--yes]          # report on whole PC folders; --yes deletes them and updates the database
filescan --config config.yml run                 # older merge pipeline (non-destructive)
filescan --config config.yml scan
filescan --config config.yml duplicates          # reads only files with no hash yet; --rescan rebuilds the groups
filescan --config config.yml similarity
filescan --config config.yml plan
filescan --config config.yml report
filescan --config config.yml execute --plan /path/to/plan.json
filescan --config config.yml waste
filescan --config config.yml largest
filescan --config config.yml merge-review
filescan --config config.yml drives              # check each PC root is on the right drive (volume serial)
filescan --config config.yml import-server       # list server roots with fsagent over SSH
filescan --config config.yml exclusions-report   # CSV of what each exclusion rule leaves out
filescan --config config.yml compare             # PC files with no identical copy on the server
filescan --config config.yml hash-server         # hash server files that share a size, on the server
filescan --config config.yml unwanted-report     # unwanted-data rules, reported by folder
filescan --config config.yml review server-trash # keep or trash, folder by folder, for one exclusion
filescan --config config.yml measure             # distinct size of the organised set against disk space
filescan --config config.yml cleanup-plan        # files marked trash or matched by delete-candidate rules (CSV)
filescan --config config.yml cleanup --plan DIR  # move a reviewed plan's files to quarantine (reversible)
filescan --config config.yml cleanup-restore --plan DIR
filescan --config config.yml cleanup-purge --plan DIR --yes  # delete quarantined files after 14 days

poetry run pytest tests                          # run all tests
poetry run pytest tests/unit/test_config.py      # one file
poetry run pytest tests/unit/test_config.py::test_load_config_uses_paths_and_defaults

flake8 src/
isort src/ tests/
black src/ tests/
```

The live configuration and database are in `C:\filescan` (`config.yml`, `file_index.db`).

## Architecture

**Entry point:** `src/filescan/cli.py` parses subcommands, runs the preflight database checks, then dispatches to the pipeline stages.

| Stage | Module | Key class |
|-------|--------|-----------|
| Scan | `inventory/scanner.py` | `InventoryScanner`: multi-threaded recursive walk, writes `folders`/`files` |
| Deduplicate | `dedupe/duplicates.py` | `DuplicateDetector`: size group, then quick hash, then full hash |
| Similarity | `similarity/folders.py` | `FolderSimilarityAnalyser`: weighted heuristics (file signatures 35%, shared duplicates 55%, name 10%) |
| Clusters | `similarity/clusters.py`, `similarity/merge_review.py` | Near-duplicate folder clusters and the Textual review TUI |
| Plan | `planning/proposals.py` | `ProposalBuilder`: `mark_backup`, `merge_folder`, `needs_review` |
| Report | `reporting/xlsx.py` | Custom XLSX writer (raw ZIP and XML, no external library) |
| Execute | `execution/mover.py` | `ExecutionRunner`: copies trees, verifies by hash before writing |
| Waste | `cleanup/waste_detector.py` | Pattern-based detection of junk and cache folders |
| Cleanup | `cleanup/plan.py`, `cleanup/quarantine.py` | Plan from review decisions and rules; quarantine on the same disk, restore, purge. Server files move through `fsagent move` and `fsagent remove` |

**Data layer:** `storage/db.py` (`SQLiteDB`, WAL mode) and `storage/repositories.py` (`FileRepository`). The database is the single source of truth for restartability: stages check what has already been processed and skip it unless `--rescan` is given.

**Models:** `models.py` defines the data classes (`FileRecord`, `FolderRecord`, `DuplicateGroup`, `FolderSimilarityCandidate`, `PlanProposal`, `ExecutionResult`, `ScanConfig`).

**Hashing:** `dedupe/hashing.py`. The quick hash reads a 4 KB head, a 4 KB tail and the size. The full hash streams the whole file in 1 MB chunks with xxhash128.

**Server helper:** `server/fsagent.py` runs on the Debian server. It lists, hashes and copies files locally, and filescan drives it over SSH. `inventory/server_import.py` streams it to the server's `python3` on stdin, runs `list`, and stores the results under UNC paths. Server roots in `config.yml` need `location: server` and `server_path` (the POSIX mount point); the top-level `server: {ssh: ...}` gives the SSH target.

## Product rules

- filescan runs on Windows only. The server helper `server/fsagent.py` targets Debian and uses only the Python standard library and `xxhash`.
- Configure server roots as UNC paths (`\\192.168.1.133\disk3`), never as mapped drive letters: an elevated shell cannot see drives mapped in a normal session.
- Keep the SQLite database on a local disk, never on a network share.
- Suggestions must be reviewed by the user before any file operation.
- Never delete source data in the normal pipeline. Deletion is always a separate, explicit command: `cleanup-purge --yes` acts on a reviewed plan, and `delete-folder --yes` acts on folders the user names after reading its report. `delete-folder` never goes through a junction or symbolic link, and never changes permissions on C:.
- Treat exact duplicate files and similar folders as separate concerns.

## Analysis rules

- Exact duplicate detection is independent of folder similarity detection.
- Folder similarity uses several heuristics, not Jaccard alone.
- Backup-like folder names include `backup`, `copy`, `old`, `temp` and `temporary`.
- Categorisation suggestions may use metadata or content analysis, but the user makes the final decisions.
- Two files count as the same only when their content hashes match. Name, size or relative path alone can suggest a match for review, never prove one.

## UX rules

- Always show the active process during long-running work.
- Every long-running loop uses a visible `tqdm` progress bar with `leave=True`.
- With no command, filescan prints the help, with the commands grouped by stage. `run` runs the older non-destructive merge pipeline.
- `run` ends with a summary and a report in the configured filescan folder.
- `filescan status` only reads; it never runs a stage. Keep its to-do list in step with the phases in PLAN.md.

## Config rules

- Related outputs share one `filescan_folder`.
- Database and report locations come from `database_folder`, `database_filename` and `report_filename`.
- The configuration file comes from `--config`, then `FILESCAN_CONFIG` in the environment, then `FILESCAN_CONFIG` in `.env` at the repository root. `.env` is not committed; `.env.example` shows the format.

## Engineering rules

- Use `pathlib.Path` rather than string paths or the `os` module where possible. Exception: the scanner may use `os.scandir`, because on Windows its `DirEntry` carries size and mtime from the directory listing and saves a round trip per file.
- Follow TDD with `pytest`. Unit tests live in `tests/unit/` (fast, isolated); integration tests live in `tests/integration/` (full pipeline on temporary file trees).
- Include a test that builds synthetic data with duplicate files and folders, and checks that the Excel report has a row for each duplicate. Keep that data and its report in `tests/manual/report_fixture/` for manual inspection.
- Keep startup checks fast, and fail early if the configured database is not writable.
- Make the code restartable: a crash should lose at most the current batch.
- Don't rescan or re-analyse folders already processed unless `--rescan` is given.
- Put safety and observability ahead of raw speed.
- Catch `PermissionError` and other `OSError`s on individual files, log them and continue.
- When building SQL `LIKE` patterns from paths, escape `_`, `%` and `\`.

## Test structure

- `tests/unit/`: isolated unit tests, no disk I/O beyond temporary fixtures.
- `tests/integration/`: full CLI pipeline tests using `tree_builder.build_tree()` to create synthetic file trees.
- `tests/manual/report_fixture/`: a persistent dataset for manual XLSX inspection. The integration test runs on a temporary copy of `input/` and leaves the tracked files alone. To refresh `output/filescan_report.xlsx` after a deliberate report change, run the test with `FILESCAN_UPDATE_FIXTURE=1`.
- `conftest.py` provides the `tmp_path` (Windows temp folder), `write_config` and `make_repo` fixtures.
- The `test_*.py` files in the repository root belong to an older prototype and are not part of the suite; run `pytest tests`.

## Working style

- Don't show code diffs in the terminal.
- Write British English in documentation, comments and commit messages.
