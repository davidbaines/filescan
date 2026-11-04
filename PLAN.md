# Plan: consolidate PC and server files into one organised set

Branch: `server_mirror`. Tick boxes as work completes, so progress survives interruptions. Steps marked **[USER]** need David to run or approve something. Steps marked **[GATE]** must pass before anything after them starts.

## Context

Files are scattered across the PC (C: user folders, D:, E:, F:), the LaCie J: and the server (disk1, disk3). Decades of backups have left overlapping copies, and each location also holds files found nowhere else. filescan already scans, hashes and merges near-duplicate folder trees on Windows. This plan extends it to cover the server and to produce one organised set.

### Agreed goals

1. **One organised set of user data.** "Organised" means exact duplicates removed and near-duplicate trees merged, under a new top level: `Photos, Videos, Music, Documents, Data, Backups, GitHub, Work, Unsorted`. Existing subtrees keep their names below that level, except Google Takeout, which is split across the categories.
2. **The PC holds everything** and is the primary copy. It may be spread over several drives, with F: (3 TB WD Red) as the main one.
3. **The server holds a complete plain-file mirror** of the PC's set, spread over several disks if necessary. No versions are kept.
4. **Task 2:** a report of which PC and J: files have no identical copy on the server.
5. **Unwanted data:** find data that can go, so the set fits on the existing disks. Rules come first. Whether to use AI (the Claude API or a local model on the 3090) is decided once we can see what the rules leave unexplained.
6. **Safety:**
   - Deletion is always a separate, explicit step.
   - Nothing is deleted from the PC until the file is in the PC's organised layout and on the server, both checked by hash.
   - All work is restartable.

### Out of scope for now

- Evelyn's data (`disk3/evelyn`). Samba shares for users david and evelyn come later.
- MT and Paratext_Projects. Backblaze hosts them, so the server doesn't need copies.
- Offsite copies, and buying drives: this plan uses the existing disks only.
- Excluded from the organised set: old OS folders, caches and tools, Acronis images, and the public corpora (Gutenberg, Corpora, eBible).
  - On the PC, excluded data becomes **deletion candidates** in a separate review report, about 790 GB. They are deleted only after approval.
  - OS exclusions are anchored to known locations, for example `^[A-Z]:\\(Windows|Program Files…)\\`, and never match a folder name anywhere in a path.
  - `.git` is **not** excluded.

### Pinned folders: mirrored, never moved or merged

- **Cloud sync folders:** `iCloudDrive`, `Desktop\Google Drive`, and any OneDrive or Dropbox folders. Moving a file out of one deletes it from the cloud.
- **Paratext and Zotero:** `D:\Paratext_Projects`, `Documents\Paratext`, `.paratext-10-studio`, `Zotero\storage`.
- **Git repositories.** Each repository is one indivisible unit. It may be moved whole to its category, but its files are never merged with other folders, and `.git` is kept.
- **Photo libraries are not pinned,** so moving `E:\Picasa` means digiKam must rescan its library afterwards.

### Measured facts (3 November 2026)

- **Server:**
  - disk1 holds 548 GB (diskb 455 GB, diskd 94 GB).
  - disk3 holds 2.5 TB, with 223 GB free.
  - disk2 is empty.
  - The server has 2 cores and 15 GiB of RAM. rsync and borg are installed; python3-xxhash and rmlint are not.
- **Estimate by name and size, user data only:**
  - Union of all locations: 3.58 TB. The floor is 2.3 TB, because renamed copies count as distinct.
  - The estimate's exclusion patterns were too broad, so the figure for PC data missing from the server may be understated.
  - Server: 2.62 TB.
  - PC with no copy on the server: 0.91 TB.
  - J: with no copy anywhere else: 37 GB.
  - disk1 with no copy on disk3: 272 GB.
  - `disk3/.Trash-1000` holds 312 GB, of which 194 GB is unique, mostly `E6510-first-disk`.
- **Hashing volumes:**
  - Only about 250 GB of server files share a size with a PC or J: file.
  - About 1.8 TB of server files share a size with another server file.
- **Other findings:**
  - `C:\Documents and Settings` is a junction to `C:\Users`, so it is excluded as a duplicate root.
  - The April database holds hashes for only 330 files, because the threshold is 1 GB.

## Architecture: the PC does the thinking, a server helper does the heavy I/O

- **PC (filescan, still Windows only).**
  - Holds the SQLite database, which stays local: `C:\filescan`.
  - Does all scanning, analysis, review (XLSX and TUI) and planning.
  - Sees the server through SMB shares: `\\192.168.1.133\disk1` and `\disk3` read-only, and `\disk2` read-write for the master.
  - Server files are stored in the database under UNC paths. Server roots are always configured as UNC paths, never as mapped drive letters.
- **Server helper (`server/fsagent.py`).** A single Python file for Debian, using only the standard library and `python3-xxhash`.
  - `fsagent list <root>` streams `path, size, mtime_ns`. filescan imports the result under the matching UNC paths. A local listing of 800k files takes about a minute.
  - `fsagent hash <list>` writes `path, size, mtime_ns, xxh128`. It is restartable: it appends to its output and skips paths already done.
  - `fsagent copy <manifest>` copies from server to server:
    - each file goes to `dst.partial`, is fsynced and re-hashed, and is renamed into place only if the hash matches;
    - it preserves mtime;
    - it writes a JSONL log and skips entries already verified.
  - A share map in `config.yml` translates `\\192.168.1.133\diskN` to `/srv/dev-disk-by-uuid-…`.
  - filescan runs it over SSH, using key login.
- **F: in bay 4 is unnecessary.** Mounting NTFS on Debian adds risk and saves little time.
- **rmlint is not needed.** Building a fresh master from one copy per hash already deduplicates within the server.

## Phases

### Phase 0: housekeeping

- [x] Commit the uncommitted `src/` changes from the cluster work.
  - **Useful:**
    - `clusters.py` counts files recursively and fixes a SQL query that could stall;
    - `merge_review.py` writes the plan atomically;
    - `proposals.py` and `clusters.py` gain progress bars;
    - `artifacts.py` picks the newest plan by mtime;
    - `mover.py` adds the source file to error messages.
  - **Concern:** `_unique_files_for_folder` treats a matching relative path and size as a copy, without checking the hash. It may stay for the TUI preview, but new commands use hashes only.
  - Do not commit `test.py`. Add `test-temp/` to `.gitignore`.
- [x] Merge the agent docs.
  - Combine `CLAUDE.md`, the root `AGENTS.md` and `src/filescan/AGENTS.md` into the root `AGENTS.md`, and delete the nested copy.
  - Reduce `CLAUDE.md` to a reference to `@AGENTS.md`.
  - Add the rules for the server work: the helper runs on Debian, server roots are UNC paths, the database never sits on a share, and deletion is always a separate command.
- [x] Commit PLAN.md and push the `server_mirror` branch.
- Known failure before any Phase 1 work: `tests/integration/test_report_fixture_contents.py` expects a `merge_folder` row in the Plans sheet and gets none. It fails on `main` too. Investigate it during Phase 1.

### Phase 1: fixes to filescan before any rescan (TDD)

- [x] **Stale hashes.** In `storage/repositories.py` `upsert_files`, delete a file's `file_hashes` row when its size or mtime changes.
- [x] **Delta scans miss in-place edits.** Resolved without a new flag: `--rescan` reads every file and, with the stale-hash fix, invalidates edited files (tested). `--delta` help now warns that it misses in-place edits.
- [x] **LIKE wildcards.** Write one helper that escapes `_`, `%` and `\` and uses `ESCAPE`, and use it in `_path_prefix`, `clusters.py` and the waste detector.
- [x] **Resolving every path.** Call `normalize_path` with `.resolve()` only for config roots. Paths read back from the database become plain `Path(...)`.
- [x] **Scanner round trips.** Switch `_scan_folder` to `os.scandir`, which is a documented exception to the pathlib rule. Drop the full pre-walk in `_count_folders` for network roots. Junctions and symlinks are now skipped rather than followed and resolved.
- [x] **Hashing at scale.** `DuplicateDetector.run` now groups by size in SQL (with a new `files(size)` index) and streams one size group at a time.
  - [ ] Set the threshold to 1 byte in `C:\filescan\config.yml` once the Phase 2 roots are configured, so the first run hashes the new inventory rather than the stale April one.
- [x] **Root roles.** Each root takes `location: pc | server` (default `pc`), `role: source | master` (default `source`) and an optional `skip` list of folders relative to the root. The config also takes top-level `exclusions` (a list of `name`, `pattern`, `reason`) and `pinned` (a list of paths).
  - [x] **Exclusions are tagged, not skipped.** Rules are evaluated when queried, not stored on folders, so editing a rule never leaves stale tags. `filescan exclusions-report` writes `exclusions_report.csv`, listing each excluded subtree once with its recursive totals, and prints totals per rule.
    - Root `skip` folders, such as AppData, are the only data never inventoried.
    - Still to do in Phase 5 and 7: the gate before any wipe reports the excluded bytes on that disk and needs an explicit sign-off.
  - [x] **`.git` removed from `exclude_folders`** in the test fixture; the code default was already empty.
    - [ ] Remove it from `C:\filescan\config.yml` while setting up the Phase 2 roots.
  - [x] **A root's location decides which stage handles it.** The scanner walks only PC roots, the duplicate detector never hashes files under server roots, and the space preflight counts only PC roots. `normalize_path` no longer resolves UNC paths, which would mean a network round trip.
    - [x] Marking missing files for server roots happens in `import-server`.
- [x] **Cloud placeholders.** The scanner stores `attributes`, and the duplicate detector skips placeholders. Listing them in the Phase 3 and exclusion reports is still to do.
  - iCloudDrive has 23 placeholder files and OneDrive 16 (`RECALL_ON_DATA_ACCESS`), and reading one downloads it.
  - Store `st_file_attributes` in the scanner, which costs nothing on Windows, and never hash files flagged `RECALL_ON_DATA_ACCESS` (0x400000), `RECALL_ON_OPEN` (0x40000) or `OFFLINE` (0x1000).
  - Report them separately. Don't skip every reparse point, because files OneDrive keeps on the device are reparse points too.
- [x] **Batch the hash commits.** Done: hashes are committed every 500 rows or 5 seconds, and one thread pool serves the whole run. Original note: `upsert_file_hash` commits after every file. Commit every few hundred files or every few seconds instead, so a crash still loses at most the current batch.
- [x] Python is pinned to 3.12 or later in `pyproject.toml`, so `DirEntry.is_junction()` is available.
- [x] **The fixture test fails on `main` too.** Fixed. Commit b168e56 deliberately turned `merge_folder` proposals into clusters, but the XLSX Plans sheet never listed clusters. The report now has `merge_cluster` rows, and the test approves clusters and runs on a copy of the fixture input; executing had been writing `.filescan-merged` markers into the tracked tree. Original note: `test_report_fixture_contents.py` expects a `merge_folder` row. Find out with `git bisect` in a separate worktree whether the cluster work changed this deliberately (update the test) or broke it (fix the code). Make the test write its report to a temporary copy, so it stops dirtying the tracked fixture.
- [x] **[GATE]** `pytest` passes: 113 tests, as of the root-roles commit.

### Phase 2: server setup and a fresh inventory

- [x] Write `server/fsagent.py` (`list`, `hash`, `copy`, `hashfile`), with tests that run on Windows.
  - `list` needs no installation: `ssh david@192.168.1.133 python3 - list <root> < server/fsagent.py` streams the script, and a trial on `diskd/music` worked.
- [ ] **[USER]** Install the helper: `sudo apt install python3-xxhash`, then copy `fsagent.py` to `~/fsagent/`.
- [ ] Check that the server's `xxh128` matches the PC's `hashing.full_hash` on the same file.
- [ ] **[USER]** Create the SMB shares in the OMV web UI: `disk1` and `disk3` read-only, `disk2` read-write.
- [ ] **[USER]** Review the exclusions report, then set the `config.yml` roots:
  - PC sources: `C:\Users\David`, `D:\`, `E:\`, `F:\`, `J:\`;
  - server sources: disk1, disk3 (Evelyn excluded);
  - master: disk2.
- [ ] `filescan scan --rescan` the PC roots.
- [ ] `filescan import-server` (runs `fsagent list`). The command is written and tested; a trial import of `diskd/music` into a scratch database gave 1,039 files, 135 folders and 0 errors. The full run waits for the config step above.
  - Server roots need `server_path` and may list `exclude_dirs` (for example `lost+found`); the SSH target is the top-level `server: {ssh: david@192.168.1.133}`.
  - A failed or empty listing raises an error before anything is marked missing.
  - Convert the server's `mtime_ns` to the stored float in one shared function. The stale-hash check compares mtimes exactly, so any drift would clear every server hash on each re-import.

### Phase 3: task 2, which files the server lacks

- [ ] Prune by size in SQL: only sizes found in both the source and the server groups become candidates.
- [ ] Hash the PC candidates locally, and the server candidates with `fsagent hash`, then import the results.
- [ ] New command `filescan compare`: an XLSX of the source files with no copy of the same content on the server, with totals per top folder.
- [ ] Run `fsagent hash` on server-only size groups, about 1.8 TB, overnight.

### Phase 4: unwanted data, rules layer

- [ ] Move the waste-detector patterns into a YAML rules file. Each rule gives a pattern, a category, a disposition and a reason.
- [ ] Add rules for installers and ISOs, VM images, build and package output, caches, Takeout sidecars, WhatsApp media and screenshots.
- [ ] Roll the results up the folder tree. Write a review report sorted by bytes reclaimable, and store review decisions in the database.
- [ ] **[USER]** Review the Trash: the 194 GB found nowhere else.
- [ ] **[USER]** Review the deletion candidates among the excluded data on the PC.
- [ ] **[GATE]** Checkpoint: measure the unique set against disk2 (overflowing to disk1) and against the PC's F:, E: and D:. Then decide whether an AI layer is needed.

### Phase 5: build the server master on disk2

- [ ] **[USER]** Write `layout.yml`, mapping each source top folder to a category. Anything unmapped goes to `Unsorted/<origin>`.
- [ ] New command `filescan build-plan`.
  - It produces one layout manifest, `hash → destination`, shared by the master and the PC.
  - Near-duplicate trees are merged with the existing cluster logic; pinned folders and Git repositories are copied whole.
  - Name conflicts follow the agreed rule, `file_from_SourceFolder.ext`.
  - It writes two copy lists: server to server for `fsagent copy`, and PC to disk2 for `ExecutionRunner`, which keeps mtimes.
- [ ] Run the copies, overflowing whole categories to disk1 if needed.
- [ ] **[GATE]** New command `filescan verify-coverage`: every kept source hash is in the master.

### Phase 6: lay out the PC's organised copy (task 3)

- [ ] New command `filescan pc-plan`.
  - It uses the same layout manifest as Phase 5.
  - Categories go to drives: F: first, overflowing to E: and D:.
  - Files already on the target drive are moved; anything else is copied from the master.
  - Pinned folders stay where they are.
- [ ] New command `filescan cleanup-plan`, with a separate `cleanup --delete` that offers a recycle-bin or quarantine option.
- [ ] Work through the drives one at a time. For each drive, in order:
  1. move files into the layout;
  2. rescan;
  3. **[GATE]** `verify-coverage`;
  4. **[USER]** approve the cleanup;
  5. copy in what's missing.
  - [ ] F:
  - [ ] E:
  - [ ] D:
  - [ ] C: user folders
  - [ ] J:
- [ ] **[USER]** Decide what happens to J: (keep it as a spare, or wipe it).

### Phase 7: backup mirror and reuse of the old disks

- [ ] **[GATE]** `verify-coverage` shows that the PC layout matches the master.
- [ ] **[USER]** Run the first PC → disk2 mirror as a preview only, with FreeFileSync or `robocopy /L`, and review it. Exclude MT and Paratext_Projects. If categories overflow to disk1, run one job per category.
- [ ] Schedule the mirror in Task Scheduler.
- [ ] **[USER]** Wipe disk3 and disk1 as a separate step and reuse them.
- Note: a mirror without versions copies accidental deletions to the server on its next run. FreeFileSync's Versioning option is a cheap safeguard.

## Verification

- Unit tests for each Phase 1 fix: stale hashes, LIKE escaping, streaming by size group, the scandir scanner.
- Integration tests with `tree_builder.build_tree()`. "pc" and "server" trees contain renamed duplicates, edited files of the same size, and unique files. The tests check that:
  - `compare` is exact;
  - `build-plan`, execution and `verify-coverage` work end to end;
  - `pc-plan` moves within a volume and copies otherwise;
  - pinned folders are never split;
  - paths and mtimes in the layout match those in the master;
  - anchored exclusions keep `GitHub\python`.
- `fsagent`: hash parity and copy resume, then a dry run on a 1 GB sample on the server.
- Before any deletion: run `verify-coverage`, review the XLSX, and spot-check 20 random files.

## Open decisions

- The AI layer: decided after the Phase 4 checkpoint.
- How Google Takeout maps to categories: decided while writing `layout.yml`.
- Which drive each category goes on: decided during the `pc-plan` review.
