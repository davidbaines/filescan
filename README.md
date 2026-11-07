# filescan

filescan brings the files on a Windows PC and a Linux home server together into one organised set. It records every file, gives each one a content hash, finds what is duplicated or unwanted, and plans where each kept file belongs. You review every plan before anything is moved, and deletion is always a separate command.

The work follows [PLAN.md](PLAN.md). Project rules for coding agents are in [AGENTS.md](AGENTS.md).

## Setting up

1. Install the dependencies: `poetry install`.
2. Copy `.env.example` to `.env` and set `FILESCAN_CONFIG` to your configuration file, for example `C:\filescan\v2\config.yml`. Every command then finds it without `--config`.
3. In `config.yml`, list the roots (PC drives and folders, and the server shares as UNC paths such as `\\192.168.1.133\disk3`), the exclusions and the pinned folders. The database and every report go in `filescan_folder`.
4. For the server, set up SSH key login and install `python3-xxhash` there. filescan sends `server/fsagent.py` to the server over SSH each time, so nothing else needs installing.
5. Write `layout.yml` in the filescan folder before running `build-plan`. It maps each source folder to its place in the organised set.

## Start here

Run `filescan status` whenever you come back to the work. It reads the database and the reports, shows what has been done, and lists what to run next, in order. It never changes anything.

Run `filescan` with no command to see every command grouped by step, and `filescan <command> -h` for a command's options.

## The steps in order

Each command says whether it only reads, writes reports, or changes files. Commands that change files are marked **moves** or **deletes**.

### 1. Inventory: record and hash every file

| Command | What it does | Changes files? |
|---|---|---|
| `filescan scan` | Records every file on the PC and the server, and gives each one a content hash. It checks each PC drive is the right one, walks the PC roots, lists the server roots over SSH, then hashes every file that has no hash yet: PC files here, two at a time per drive, and server files on the server. | No |

Run `scan` again whenever files have changed: it reads only what is new or changed, and a file whose size or date has changed gets a new hash. It is safe to stop and run again. If one part fails, for example because the server is off, the rest still runs and the summary says what to run again. Files that cannot be read are listed in the `logs` folder.

To scan only some places, comment out the other roots in `config.yml`. `--rescan` walks every PC folder again, rather than only the changed ones.

Every later step compares files by these hashes. Two files count as the same only when their hashes match.

### 2. Find what can go

| Command | What it does | Changes files? |
|---|---|---|
| `filescan exclusions-report` | Lists the folders each exclusion in `config.yml` leaves out of the organised set (old Windows folders, caches, public corpora and so on), with sizes, in `exclusions_report.csv`. | No |
| `filescan unwanted-report` | Applies the unwanted-data rules (`src/filescan/cleanup/unwanted_rules.yml`: installers, caches, temporary files, old File History versions) and reports the matches by folder in `unwanted_report.csv`. | No |
| `filescan review <exclusion>` | Opens a screen to mark each folder of one delete-candidate exclusion, such as `server-trash`, as keep or trash. It shows how much of each folder exists nowhere else. Decisions are saved as you go, in the database and in `review_<exclusion>.csv`. | No |
| `filescan measure` | Measures the organised set (each content counted once) against the free space on the PC drives and the server disks. | No |
| `filescan compare` | Lists the PC files with no identical copy on the server, in `compare_missing_files.csv`, with totals per folder. | No |

### 3. Clean up

| Command | What it does | Changes files? |
|---|---|---|
| `filescan cleanup-plan` | Lists every file in a folder you marked trash, and every file matched by a delete-candidate rule, in a new plan folder under `cleanup\`. Each file is marked "covered" if an identical copy is kept elsewhere, or "unique". | No |
| `filescan cleanup --plan <folder>` | Moves the plan's files into `_filescan_quarantine` on the same disk. `--dry-run` only counts. | **Moves** (reversible) |
| `filescan cleanup-restore --plan <folder>` | Puts a plan's quarantined files back where they were. | **Moves** |
| `filescan cleanup-purge --plan <folder> --yes` | Deletes a plan's quarantined files for good, once they have been in quarantine for 14 days. Without `--yes` it only reports. A covered file is kept if its copy has since gone. | **Deletes** |
| `filescan delete-folder <folder>` | For whole PC folders that are plainly not wanted. It reports what the folder holds and which files have no copy elsewhere; with `--yes` it deletes the folders. It refuses a folder reached through a junction or link and never follows one. `--take-ownership` fixes permissions one folder at a time, never on C:, and needs an elevated terminal. | **Deletes** with `--yes` |

The cleanup commands record every action in the plan folder, so an interrupted run carries on where it stopped.

### 4. Build the organised set

| Command | What it does | Changes files? |
|---|---|---|
| `filescan build-plan` | Reads `layout.yml` and decides where each kept file goes. It keeps one copy of each content, choosing the location from the entry that comes first in `layout.yml`, keeps Git repositories whole, and renames clashing files `name_from_<source>.ext`. The plan goes to a new folder under `build\`, with lists of what is dropped, renamed and still unsorted. | No |

Still to be built, in this order:

- `filescan similar-trees`: folder trees whose files are all contained in another tree, or nearly the same as one, for you to choose which copy to keep;
- `filescan copy-master`: copy the build plan to the server's disk2, verified by hash, keeping each file's date;
- `filescan verify-coverage`: prove by hash that every kept file is in the organised set;
- `filescan pc-plan`: lay out the PC's drives to match, moving files on the same drive and copying the rest.

After that, a plain mirror from the PC to disk2 keeps the server copy up to date.

## Where things are written

Everything goes in the configured `filescan_folder` (`C:\filescan\v2`):

- `file_index.db`: the database, the record of every file, hash and decision;
- the report CSVs named above;
- `cleanup\<plan id>\`: cleanup plans and their action logs;
- `build\<plan id>\`: build plans;
- `deletions\`: what `delete-folder` deleted;
- `logs\`: files and folders that could not be read.

## Older code

The first prototype and the older merge workflow (`run`, `similarity`, `plan`, `merge-review`, `execute`, `report`, `waste`, `largest`, `clusters`, `duplicates` and `hash-server`) are in `unused/`. They are not used, tested or maintained, and can be restored from there if needed.

## Tests

```
poetry run pytest
```

pytest runs the tests in `tests/`. A test checks that every command this README names exists.
