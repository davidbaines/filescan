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
  - `disk3/.Trash-1000` holds 312 GB, of which 194 GB is unique, mostly `E6510-first-disk`. (By hash on 4 November: 387 GB, of which 234 GB has no copy outside the Trash and the delete-candidate areas.)
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
- [x] **[USER]** Install xxhash on the server (version 3.0.0; `~/fsagent-work/` created): `sudo apt install python3-xxhash`. Nothing else needs installing, because filescan streams `fsagent.py` over SSH. `hash` and `copy` keep resumable output in a work folder on the server, `~/fsagent-work/`, which Claude creates over SSH once you approve.
- [x] Check that the server's `xxh128` matches the PC's `hashing.full_hash` on the same file. Checked on a 24.9 MB file from `diskd/music`: the hashes match.
- [x] **[USER]** Create the SMB shares in the OMV web UI. Done on 4 November: the shares expose the disk roots, `smbd` is enabled, the PC stores credentials with `cmdkey`, and a sample of 25 imported server paths matched over SMB on size and mtime.
  - All three shares are read-write for david. The plan wanted disk1 and disk3 read-only as a safety net; filescan never writes to them.
  - Windows' network provider order put WinFsp before the SMB client, and every UNC path hung. `LanmanWorkstation` now comes first.
  - The shares exist, but each points at a new empty subfolder (`<disk>/disk1/`) rather than the disk root. Each shared folder's relative path should be `/`.
  - `smbd` is inactive: enable SMB in OMV (Services > SMB/CIFS).
  - Remove the empty `disk1/`, `disk2/` and `disk3/` folders OMV created, once the shares point at the disk roots.
  - The shares are needed only from Phase 5 onwards, for copying PC files to disk2.
- [x] **[USER]** Review the draft config `C:\filescan\v2\config.yml`, which uses a **new** database in `C:\filescan\v2\`. The April config and database stay untouched for reference.
  - A new database is needed because the April one has active rows for the whole of `C:\` (Windows, Program Files, paths found by following junctions). The new roots would never mark those rows missing, so duplicate detection would hash them all.
  - Roots: PC `C:\Users\David` (AppData skipped), `D:\`, `E:\`, `F:\`, `J:\`; server sources disk1 and disk3; master disk2.
  - Evelyn, MT and Paratext_Projects are exclusions with `disposition: keep-in-place`. `.Trash-1000` is inventoried for review. `.git` is not excluded. The threshold is 1 byte.
- [x] `filescan scan --rescan` the PC roots. Done on 4 November in 49 minutes: 5.2 million files.
- [x] `filescan import-server` (runs `fsagent list`). Imported 1,301,682 files in 132,367 folders on 4 November, with no errors. One name Windows cannot represent, in the Trash, was skipped. The command is written and tested; a trial import of `diskd/music` into a scratch database gave 1,039 files, 135 folders and 0 errors. The full run waits for the config step above.
  - Server roots need `server_path` and may list `exclude_dirs` (for example `lost+found`); the SSH target is the top-level `server: {ssh: david@192.168.1.133}`.
  - A failed or empty listing raises an error before anything is marked missing.
  - Convert the server's `mtime_ns` to the stored float in one shared function. The stale-hash check compares mtimes exactly, so any drift would clear every server hash on each re-import.
- [x] **Record what the import couldn't take.** Done 8 November: an `access_problems` table holds what `scan`, `import-server` and hashing could not read, including server names Windows cannot represent; each step replaces only what it tried again. The workbook's Unreadable sheet lists them by folder, with files cleanup could not quarantine. filescan does not change permissions; the user deletes or deals with these folders.
  - [ ] Make the gate before any wipe count them alongside the excluded bytes.

### Phase 3: task 2, which files the server lacks

- [x] Prune by size: only sizes found on both the PC and the server become candidates (`filescan compare`).
- [x] Hash the PC candidates locally, and the server candidates with `fsagent hash` (`inventory/server_hash.py`), then import the results. A server hash is stored only if the file's size and mtime still match the inventory, and `fsagent hash` re-hashes files changed since an earlier result.
  - [x] Run it for real once the inventory is complete. Done on 4 November in 6 hours: the PC hashed 3.83 million files in 4 h 10 min, and the server hashed 1.17 million, with no stale results or errors.
    - PC files with no identical copy on the server, outside the exclusions: 1.96 TB in 2.15 million files. Counted once per name and size, that is about 1.0 TB, close to the earlier 0.91 TB estimate. Nearly all of it (1.91 TB) has no file of the same size on the server.
    - Largest top folders: `F:\Data` 348 GB, `J:\Photos` 286 GB, `C:\Users\David\Pictures` 276 GB, `F:\Photos` 265 GB, `E:\Picasa` 202 GB.
    - 195,795 PC files could not be read (permission denied), almost all under `D:\Windows` and `D:\Program Files`, which are excluded anyway.
    - Exclusions report: public corpora 536 GB, Acronis 440 GB, Evelyn 182 GB, app caches 128 GB, old OS root 108 GB, Backblaze-hosted 103 GB, old OS in backups 61 GB.
- [x] **Let duplicate groups include server files.** Done: server files with a full hash join their size group, which is then compared on full hashes. Server files without one are left out and never read. Today the detector leaves server files out of grouping entirely, not just out of hashing. Similarity (55% weight on shared duplicates) and clusters would then never link a PC tree to a server tree, which breaks Phase 5.
  - Rule: never compute a hash for a server file, but do group server files that already have a full hash from `fsagent`.
  - `fsagent` supplies only full hashes, so a size group that includes server files skips the quick-hash prefilter for them.
- [x] New command `filescan compare`. It writes `compare_missing_files.csv` (status, exclusion rule, size, path) and `compare_summary.csv` (totals per top folder, with excluded bytes split out) rather than XLSX, because the file list may run to millions of rows. Statuses are `no-size-match`, `content-differs`, `not-hashed` and `cloud-placeholder`.
- [x] Run `fsagent hash` on server-only size groups (`filescan hash-server`). Done on 4 November, 10:36 to 14:44: 86,154 files, 1.61 TB, all stored, with no stale results or errors. disk3 averaged 110 MB/s.

### Phase 4: unwanted data, rules layer

- [x] Put the unwanted-data rules in a YAML file, `src/filescan/cleanup/unwanted_rules.yml`. Each rule gives a pattern, a category, a disposition and a reason. The older `waste` command keeps its own patterns, because it serves a different purpose (`.url` shortcuts to cache folders).
- [x] Add rules for installers and ISOs, VM images, build and package output, caches, Takeout sidecars, WhatsApp media and screenshots, plus a version rule for Windows File History copies.
  - `filescan unwanted-report` writes `unwanted_report.csv` (by folder) and `unwanted_files.csv` (every file), and stores the matches in `unwanted_matches`.
  - First run, 4 November: older File History versions 1,565 GB (all on `disk3\david\David\WORK`, where the newest versions need 82.5 GB); disk images 56 GB; VM disks 54 GB; installers 32 GB; build output 23 GB; caches 10 GB; the rest is small.
- [x] Roll the results up the folder tree, and store review decisions in the database. `filescan review <scope>` reviews one delete-candidate exclusion folder by folder; decisions go to `review_decisions` and `review_<scope>.csv`.
  - A file counts as unique unless a copy survives outside every delete-candidate area.
  - Tools considered: the existing Textual screen (chosen: one key or click per folder, runs in the VS Code terminal, stores each decision immediately); links in a Markdown or CSV file (two actions per decision, and folder links behave unpredictably); WizTree, TreeSize, dupeGuru and Czkawka (good for browsing, but none records a keep or trash decision per folder against our hashes).
- [x] `server-trash` added to the exclusions in `C:\filescan\v2\config.yml` (first in the list, so the whole Trash is one review scope). The previous config is `config.yml.bak-20261104`.
- [ ] **[USER]** Review the Trash: `filescan --config C:\filescan\v2\config.yml review server-trash`. 235 GB is unique, in 595 folders; 67 folders (64 GB) carry a "trash" suggestion from the old-OS and cache rules. A copy counts as kept only outside every delete-candidate exclusion and every unwanted-data match.
- [ ] **[USER]** Review the deletion candidates among the excluded data on the PC: the same command with `acronis`, `public-corpora`, `old-os-root`, `old-os-in-backups` or `app-caches`. As of 6 November all are decided except 3 folders in `public-corpora` (and 1 in `server-trash`); `filescan status` shows the counts.
- [x] **Simpler use of the CLI**, 6 November:
  - `filescan status` reads the database, the review CSVs, the cleanup plans and `layout.yml`, and lists what to do next. It never runs anything.
  - The configuration file comes from `FILESCAN_CONFIG` in `.env` at the repository root (or the environment), so `--config` can be left out. There is no built-in default.
  - The help groups the commands by stage, lists the planned Phase 5 and 6 commands, and moves the older merge commands to the end. With no command, filescan prints the help instead of running the old pipeline.
- [x] **`filescan delete-folder <folder>... [--yes]`**, 6 November, for folders that are plainly not wanted. Without `--yes` it reports what the database holds (with and without a copy kept elsewhere), what is on disk now, the Git repositories and links inside, and lists the files with no known copy in a CSV. With `--yes` it deletes for good and marks the files missing in the database; a log goes to `deletions\`. It never goes through a junction or link (an old profile has junctions into the live one), refuses roots, pinned folders and server folders, and with `--take-ownership` fixes permissions one folder at a time, never on C:.
- [x] **Hashing work made visible**, 6 November. A measurement showed that hashes are not recalculated: `scan --rescan` keeps a file's hash unless its size or date changed, quarantined files are never scanned, and `duplicates` reads only files with no hash. The hours on 4 and 5 November were first-time hashing of 3.25 million files. Changes:
  - `duplicates` says up front how many files are already hashed and how many are not, and its progress bars count only files actually read;
  - a size group whose files all have full hashes is compared without reading anything, which saves about 38,000 quick hashes;
  - each run saves what it read to `duplicates.json`, and `status` shows it, along with a reminder when server hashes have been added since the last run.
- [x] **[USER]** Decide whether older File History versions can go. Decided 5 November: yes, keep only the newest version of each file. The rule `file-history-old-versions` is now a delete candidate (40,541 files, 1,455 GB distinct); nothing is deleted until a separate cleanup command runs.
- [ ] **[USER]** Review the rest of the unwanted-data report (`unwanted_report.csv`): disk images, VM disks and installers.
- [ ] **[GATE]** Checkpoint: measure the unique set against disk2 (overflowing to disk1) and against the PC's F:, E: and D:. Then decide whether an AI layer is needed. `filescan measure` does the measuring; see the figures below.
  - Final figures, 5 November, after `duplicates` hashed every PC size group and with the File History decision applied (distinct content):

    | | Total | PC only | Server only | Both |
    |---|---|---|---|---|
    | Organised set, as it stands | 3,180 GB | 1,014 GB | 2,089 GB | 76 GB |
    | Without delete-candidate matches | 1,701 GB | 996 GB | 629 GB | 76 GB |
    | Without any unwanted-data match | 1,590 GB | 951 GB | 564 GB | 75 GB |

  - Excluded areas (the Trash, Acronis, old OS folders, corpora) are not in these figures except folders marked keep in a review; none are marked yet.
  - Against the targets: disk2 has 2,952 GB free, so 1.7 TB fits with room for kept Trash folders. On the PC, 629 GB of server-only data must come over; F: has 962 GB free before any cleanup.
  - Conclusion: the rules alone make the set fit, so no AI layer is needed for space. It could still help with sorting `Unsorted`.

### Phase 4b: drive identity and more external drives

Built 8 November, while two cleanup plans are in quarantine: the change only adds a table and a marker check, and a root without a marker still passes on its serial, so the plans in use are unaffected. Two more drives are due: a WD My Book Black on USB (K:) and a networked WD My Book White.

Decided on 6 November:

- **The drive letter in the config stays a drive's name in the database.** Paths stay absolute (`J:\…`), so review decisions, exclusion patterns, pinned folders, `layout.yml` and existing plans keep working.
- **No following a drive to another letter.** If a drive turns up on the wrong letter, filescan finds it by its marker file, names the letter it is on, and asks you to change it back in Disk Management. Until then, that root is skipped and its records are left unchanged, so no file is marked missing because its drive moved. Without translation there is nothing to time, so no speed test is needed.
- **A marker file on each drive is its identity.** The file is `.filescan-volume.yaml` at the volume's root. It holds:
  - a volume ID that filescan generates;
  - the friendly name and the expected letter;
  - the fingerprint: volume serial, size, label, filesystem, partition GUID where there is one, and the disk's model, serial and bus type;
  - when it was written.

  If a marker's fingerprint doesn't match the drive it sits on, it has been copied from another drive, for example by a drive clone or by the server mirror. filescan then treats the drive as unknown and says so.
- **Network shares use the same marker.** The My Book White gets one at the root of its share. A share has no disk details, so the marker's volume ID is its identity.
- **A reformatted drive is a new volume.** It has a new serial and no marker, so it is registered as a new drive.

To build:

- [x] Check each PC root's volume serial before scanning or cleanup, and skip a root on the wrong drive without marking its files missing. `filescan drives` lists the roots and their serials. Done 6 November; names and serials are in `C:\filescan\v2\config.yml`.
- [x] A `volumes` table: volume ID, fingerprint, expected letter, the letter last seen and when. The config holds what you declare (letter, name, volume ID); the database holds what filescan observes.
- [x] `filescan drives` lists every connected volume, with its letter, size, label and file system (model and bus type need elevation, so they are left out), and marks the ones the config doesn't know. For a new one it asks for a friendly name, writes the marker and adds the root to `config.yml`. It also takes a one-line form, `drives --add K: --name "WD My Book Black"`.
  - Insert the new lines as text, keeping your comments. Back up the config and show the change before writing.
- [x] The scan and the cleanup commands check the marker as well as the serial. A root on the wrong letter is skipped with a message naming the letter it is on now.
- [x] Leave markers out of the inventory (and so out of the master build).
  - [ ] Leave them out of the PC → disk2 mirror (Phase 7).
- [x] C: is the system drive, and writing to `C:\` needs an elevated terminal. Decided 8 November: C: gets no marker and is checked by serial only, since nothing on C: changes without the user's approval.
- [x] A way to retire a drive that has gone for good or been reformatted, such as `drives --retire "LaCie Quadra"`. It marks the drive's files missing, so they no longer count as copies.
- [ ] **[USER]** Give the WD My Book Black the letter K: in Disk Management, then register it with `drives`, scan it and run `duplicates`.
- [ ] **[USER]** Check whether Explorer can open the My Book White's share. Older models only offer SMB1, which Windows 10 turns off. If so, choose between turning on the SMB1 client for the copy and mounting the share on the server for `fsagent` to list.

### Phase 5: build the server master on disk2

- [x] **[USER]** Write `layout.yml`, mapping each source top folder to a category. Anything unmapped goes to `Unsorted/<origin>`.
  - Done 6 November: `C:\filescan\v2\layout.yml` has 59 mappings and 10 folders that stay in place, and every destination is in `root_categories`.
- [x] New command `filescan build-plan`, 7 November.
  - It produces one layout manifest, `hash → destination`, shared by the master and the PC, in `build\<plan id>\` and the `build_manifest` table.
  - One copy of each content; the entry that comes first in `layout.yml` keeps it (decided 7 November). Folders mapped to the same place merge by name, which replaces the cluster logic.
  - Git repositories (a folder with `.git\HEAD`) are kept whole and stay out of the deduplication. Pinned and `stay_in_place` folders are left to the Phase 7 mirror (decided 7 November).
  - Name conflicts become `file_from_<label>.ext`, with the label taken from the drive and path (`F-Data-D-Work`), because most merged sources share their last folder name.
  - It writes two copy lists: server to server for `fsagent copy` (`server_copy.jsonl`), and PC to disk2 (`pc_copy.csv`). Each copy takes the kept file's mtime.
  - First real plan, `20261107-111552`: 1,670,517 files, 2,125 GB (740 GB copied within the server, 1,386 GB from the PC). Unsorted 453 GB, of which 337 GB is the two Acronis folders kept in review. 354,843 kept files (1,237 GB) have no full hash, mostly because their size is unique; they need one before verify-coverage.
- [ ] **[USER]** Review the plan: the workbook's Build summary, Unsorted and Renamed sheets (since 8 November `build-plan` adds them to `filescan_report.xlsx`). The figures from 7 November predate the full hashing and the Downloads change, so run `build-plan` again first.
- [x] **Simplified, 7 November.** `hash-all` gives every file a full hash, so `duplicates` and `hash-server` are retired and `compare` only reads. The older merge workflow and the first prototype moved to `unused/`. README.md gives the order of the commands. `scan` now runs the whole inventory (drives, scan-pc, import-server, hash-all) and then writes every report to one workbook, `filescan_report.xlsx`; `report` rewrites them without scanning. The single steps remain for when only one is needed. A review of the whole codebase led to fixes in `delete-folder` (a folder reached through a junction above it is refused, and `takeown`/`icacls` refuse any target on C: or through a link) and in the quarantine (a failed restore or purge can be run again; a purge checks the copy's hash and date).
- [ ] New command `similar-trees`: folder trees contained in, or nearly the same as, another tree, for review. Then measure one copy per tree against one copy per file, and decide.
- [ ] New command `copy-master`: run the copy lists, verified by hash, keeping each file's date.
- [ ] Run the copies, overflowing whole categories to disk1 if needed.
- [ ] **[GATE]** New command `filescan verify-coverage`: every kept source hash is in the master.

### Phase 6: lay out the PC's organised copy (task 3)

- [ ] New command `filescan pc-plan`.
  - It uses the same layout manifest as Phase 5.
  - Categories go to drives: F: first, overflowing to E: and D:.
  - Files already on the target drive are moved; anything else is copied from the master.
  - Pinned folders stay where they are.
- [x] Cleanup commands, built 5 November. Quarantine is the default; the user chose it over the Recycle Bin, which does not cover network shares and deletes outright anything too large for it.
  - `filescan cleanup-plan [--only pc|server] [--no-rules]` lists every file under a folder marked trash in a review, plus files matched by delete-candidate rules. It works file by file, as at the last scan. It never includes pinned folders, keep-in-place exclusions, cloud placeholders, files under a folder marked keep, or rule matches inside Git repositories. Each file is "covered" (an identical copy is kept outside the plan and every doomed area) or "unique". Output: `cleanup\<plan id>\summary.csv` and `files.csv`.
  - `filescan cleanup --plan <folder> [--dry-run]` renames each file into `<root>\_filescan_quarantine\<plan id>\…` on the same disk, only if its size and mtime still match; it never copies across disks or overwrites. Server files go through `fsagent move` over SSH. Moved files are marked missing in the database; the scanner and `fsagent list` skip the quarantine folder.
  - `filescan cleanup-restore --plan <folder>` puts files back.
  - `filescan cleanup-purge --plan <folder> [--older-than-days 14] --yes` deletes quarantined files for good; a covered file is kept if its copy has gone. Without `--yes` it only reports.
  - Every action is logged in `actions.jsonl` in the plan folder, so each command resumes after an interruption.
- [ ] **[USER]** Review the first plan, then quarantine the server Trash and the reviewed excluded areas.
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
