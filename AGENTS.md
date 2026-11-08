# AGENTS.md

Guidance for coding agents working in this repository. `CLAUDE.md` points here, and this file is the single source of project rules.

## Project intent

`filescan` inventories the files on the PC and the server, gives every file a content hash, and builds one organised set of files from them, with a review step before anything is moved or deleted. [README.md](README.md) gives the order of the commands and what each one does.

Nothing is deleted or moved without user approval. Deletion is always a separate, explicit command.

The older merge workflow (`run`, `similarity`, `plan`, `merge-review`, `execute`, `report`, `waste`, `largest`, `clusters`, `duplicates`, `hash-server`) and the first prototype are in `unused/`. They are not imported, tested or maintained.

The current work extends filescan to compare the PC with a Linux home server and to build one organised set of files. That work happens on the `server_mirror` branch, and [PLAN.md](PLAN.md) describes it, with checklists recording progress. Read PLAN.md before starting work on that branch and tick items off as they are completed.

## Commands

```bash
poetry install                                   # install dependencies

# --config can be left out when FILESCAN_CONFIG is set in the environment or in .env at the
# repository root (copy .env.example). There is no built-in default. With no command,
# filescan prints the help, grouped by stage.
filescan status                                  # where the work stands and what to run next (reads only)
filescan scan [--rescan]                         # the whole inventory, then report; repeat scans read only changed folders unless --rescan
#   the single steps, for when only one is needed (not in README.md):
#   filescan drives | scan-pc | import-server | hash-all
filescan report                                  # the reports again without scanning: one workbook plus CSVs
filescan review server-trash                     # keep or trash, folder by folder, for one exclusion
filescan cleanup-plan                            # files marked trash or matched by delete-candidate rules (CSV)
filescan cleanup --plan DIR                      # move a reviewed plan's files to quarantine (reversible)
filescan cleanup-restore --plan DIR
filescan cleanup-purge --plan DIR --yes          # delete quarantined files after 14 days
filescan delete-folder "D:\Old" [--yes]          # report on whole PC folders; --yes deletes them
filescan build-plan                              # where each kept file goes in the layout (layout.yml); copies nothing

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
| Inventory | `inventory/full_scan.py` | `run_full_scan`: the steps of `scan` in turn; a failed step is reported and the rest still run |
| Scan the PC | `inventory/scanner.py` | `InventoryScanner`: multi-threaded walk of the PC roots, writes `folders`/`files` |
| Server import | `inventory/server_import.py` | `ServerImporter`: runs `fsagent list` over SSH and stores server files under UNC paths |
| Hash | `dedupe/hash_all.py`, `inventory/server_hash.py` | A full hash for every file: PC files here, two per drive; server files by `fsagent hash` |
| Exclusions and unwanted data | `exclusion_index.py`, `reporting/exclusions.py`, `cleanup/unwanted.py` | Rules from `config.yml` and `cleanup/unwanted_rules.yml`, applied by folder |
| Review | `review/` | Textual screen: keep or trash, folder by folder, for one delete-candidate exclusion |
| Reports | `reporting/report.py`, `reporting/xlsx.py`, `reporting/measure.py`, `reporting/exclusions.py`, `cleanup/unwanted.py`, `compare.py` | `run_reports_for_config` writes each report as CSV, then one workbook (standard library only); all read the database, and unwanted also stores its matches |
| Cleanup | `cleanup/plan.py`, `cleanup/quarantine.py`, `cleanup/delete_folder.py` | Plan from review decisions and rules; quarantine on the same disk, restore, purge. Server files move through `fsagent move` and `fsagent remove` |
| Build plan | `planning/layout.py`, `planning/build_plan.py` | `load_layout` reads and checks `layout.yml`; `build` keeps one copy of each content in layout order, keeps Git repositories whole and writes the manifest |
| Status | `status.py` | What has been done and what to run next; reads only |

**Data layer:** `storage/db.py` (`SQLiteDB`, WAL mode) and `storage/repositories.py` (`FileRepository`). The database is the single source of truth for restartability: stages check what has already been processed and skip it unless `--rescan` is given.

**Models:** `models.py` defines the data classes (`FileRecord`, `FolderRecord`, `RootConfig`, `ExclusionRule`, `ScanConfig`).

**Hashing:** `dedupe/hashing.py`. The full hash streams the whole file in 1 MB chunks with xxhash128. `hash_if_unchanged` stores a hash only if the file's size and date match the scan before and after reading.

**Server helper:** `server/fsagent.py` runs on the Debian server. It lists, hashes and copies files locally, and filescan drives it over SSH. `inventory/server_import.py` streams it to the server's `python3` on stdin, runs `list`, and stores the results under UNC paths. Server roots in `config.yml` need `location: server` and `server_path` (the POSIX mount point); the top-level `server: {ssh: ...}` gives the SSH target.

## Product rules

- filescan runs on Windows only. The server helper `server/fsagent.py` targets Debian and uses only the Python standard library and `xxhash`.
- Configure server roots as UNC paths (`\\192.168.1.133\disk3`), never as mapped drive letters: an elevated shell cannot see drives mapped in a normal session.
- Keep the SQLite database on a local disk, never on a network share.
- Suggestions must be reviewed by the user before any file operation.
- Never use recursive permission commands (`takeown /r`, `icacls /t`, Explorer's "replace all child object entries") on user data, and never generate a script that does. They follow junctions and symbolic links. On 6 November a generated `fix_permissions.bat` ran them on `D:\Users`, went through old-profile links into `C:\ProgramData`, `C:\Users\Default`, `C:\Users\Public` and `C:\Users\David\AppData\Local`, and changed owners and reset permissions there. filescan does not change permissions at all (decided 8 November; `delete-folder --take-ownership` was removed). Files and folders it cannot read are stored in `access_problems` and listed on the workbook's Unreadable sheet for the user to deal with.
- Never delete source data in the normal pipeline. Deletion is always a separate, explicit command: `cleanup-purge --yes` acts on a reviewed plan, and `delete-folder --yes` acts on folders the user names after reading its report. `delete-folder` never goes through a junction or symbolic link, refuses a folder reached through one, and never changes permissions.
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
- Never print inside a loop that has a progress bar. Problems with single files or folders go to `SkipLog` (`progress.py`), which writes them to `<filescan folder>\logs\`, shows a running count beside the bar and prints totals by reason once the bar has closed.
- With no command, filescan prints the help, with the commands grouped by stage in the order README.md describes. A test checks that every command README.md names exists.
- `filescan status` only reads; it never runs a stage. Keep its to-do list in step with the phases in PLAN.md.

## Config rules

- Related outputs share one `filescan_folder`.
- The database location comes from `database_folder` and `database_filename`.
- The configuration file comes from `--config`, then `FILESCAN_CONFIG` in the environment, then `FILESCAN_CONFIG` in `.env` at the repository root. `.env` is not committed; `.env.example` shows the format.

## Engineering rules

- Use `pathlib.Path` rather than string paths or the `os` module where possible. Exception: the scanner may use `os.scandir`, because on Windows its `DirEntry` carries size and mtime from the directory listing and saves a round trip per file.
- Follow TDD with `pytest`. Unit tests live in `tests/unit/` (fast, isolated); integration tests live in `tests/integration/` (full pipeline on temporary file trees).
- Include an end-to-end test that builds synthetic data with duplicate files and folders and checks the build plan keeps one copy of each (`test_build_plan_end_to_end`). The older Excel report fixture moved to `unused/` with the merge workflow.
- Keep startup checks fast, and fail early if the configured database is not writable.
- Make the code restartable: a crash should lose at most the current batch.
- Don't rescan or re-analyse folders already processed unless `--rescan` is given.
- Put safety and observability ahead of raw speed.
- Catch `PermissionError` and other `OSError`s on individual files, log them and continue.
- When building SQL `LIKE` patterns from paths, escape `_`, `%` and `\`.

## Test structure

- `tests/unit/`: isolated unit tests, no disk I/O beyond temporary fixtures.
- `tests/integration/`: full CLI pipeline tests using `tree_builder.build_tree()` to create synthetic file trees.
- `conftest.py` provides the `tmp_path` (Windows temp folder), `write_config` and `make_repo` fixtures.
- `pytest` collects only `tests/` (`testpaths` in `pyproject.toml`); the tests in `unused/` are not run.

## Working style

- Don't show code diffs in the terminal.
- Write British English in documentation, comments and commit messages.
