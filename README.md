# filescan

filescan brings the files on a Windows PC and a Linux home server together into one organised set. It records every file, gives each one a content hash, finds what is duplicated or unwanted, and plans where each kept file belongs. You review every plan before anything is moved, and deletion is always a separate command.

The work follows [PLAN.md](PLAN.md). Project rules for coding agents are in [AGENTS.md](AGENTS.md).

## Installation and initial configuration

To install filescan on the PC and run it from there:

1. Install Python 3.12 or later. filescan does not install on earlier versions.
2. Install Poetry. Version 1.7.1 was used during development; other versions should work.
3. Clone the repository, change to its folder and install the dependencies:

   ```
   git clone https://github.com/davidbaines/filescan
   cd filescan
   poetry install
   ```

4. Copy `.env.example` to `.env` in the repository's root folder, and in it set `FILESCAN_CONFIG` to the path of your configuration file. filescan opens that file unless another is given with `--config`.
5. Create a folder for filescan, such as `C:\filescan`, put the configuration file `config.yml` in it, and set `filescan_folder` in the configuration file to that folder. filescan saves its database there (unless `database_folder` names another folder) and writes every report there, which keeps the files it creates apart from your own.
6. In the configuration file, list under `roots` the drives and folders to organise, for example `D:\` or `C:\Users\David`. A server share is a UNC path with two leading backslashes, such as `\\192.168.1.100\Photos`, and its entry also needs `location: server` and `server_path`, the folder's path on the server. Folders that should not be scanned at all go in a PC root's `skip` list, or in a server root's `exclude_dirs`.
7. Add to `pinned` any folders that must never be moved, such as cloud-sync folders. Add to `exclusions` the folders to leave out of the organised set, such as old Windows folders and caches. Excluded folders are still scanned and appear in the reports, so you can review them before anything is deleted.
8. On the server, set up SSH key login from the PC and install `python3-xxhash`, so that file hashes can be calculated there. Put the SSH target in the configuration file under `server: ssh:`, for example `david@192.168.1.100`. filescan sends `server/fsagent.py` to the server over SSH each time, so nothing else needs installing.
9. `layout.yml`, in the filescan folder, maps each source folder to its place in the organised set. It needs to be correct before you run `build-plan`.
10. Run `filescan status`. It reads the database and the reports, shows what has been done and lists what to do next. It changes nothing.
11. Run `filescan --help` for a list of the commands, and `filescan <command> --help` for a command's options. The rest of this README describes each step.

## The steps in order

Each command says whether it only reads, writes reports, or changes files. Commands that change files are marked **moves** or **deletes**.

### 1. Inventory and reports

| Command | What it does | Changes files? |
|---|---|---|
| `filescan scan` | Records every file on the PC and the server, and gives each one a content hash. It checks each PC drive is the right one, walks the PC roots, lists the server roots over SSH, then hashes every file that has no hash yet: PC files here, two at a time per drive, and server files on the server. When it finishes, it writes the reports and lists what it wrote. | No |
| `filescan report` | Writes the reports again without scanning, for example after changing the unwanted-data rules. | No |

Run `scan` again whenever files have changed: it reads only what is new or changed, and a file whose size or date has changed gets a new hash. It is safe to stop and run again. If one part fails, for example because the server is off, the rest still runs, and the summary and the workbook say which part failed. Files that cannot be read are listed in the `logs` folder.

To scan only some places, comment out the other roots in `config.yml`. `--rescan` walks every PC folder again, rather than only the changed ones.

Every later step compares files by these hashes. Two files count as the same only when their hashes match.

The reports go into one workbook, `filescan_report.xlsx` in the filescan folder (`report_filename` in `config.yml` changes the name), with a Contents sheet and one sheet per report:

| Sheet | What it shows |
|---|---|
| Measure | The size of the organised set, each content counted once, against the free space on the PC drives and the server disks. |
| Exclusions | The folders each exclusion in `config.yml` leaves out of the organised set (old Windows folders, caches, public corpora and so on), with sizes. |
| Unwanted | Matches of the unwanted-data rules (`src/filescan/cleanup/unwanted_rules.yml`: installers, caches, temporary files, old File History versions), by folder. |
| Missing from server | PC files with no identical copy on the server, totalled by top folder. |

Each sheet is also written as a CSV. Two lists are CSV only, because they can be longer than Excel allows (1,048,576 rows): every file an unwanted-data rule matches (`unwanted_files.csv`), and every PC file with no copy on the server (`compare_missing_files.csv`). If the workbook is open in Excel when the reports are written, they go to a copy with the date and time in its name.

### 2. Decide what can go

| Command | What it does | Changes files? |
|---|---|---|
| `filescan review <exclusion>` | Opens a screen to mark each folder of one delete-candidate exclusion, such as `server-trash`, as keep or trash. It shows how much of each folder exists nowhere else. Decisions are saved as you go, in the database and in `review_<exclusion>.csv`. | No |

### 3. Clean up

| Command | What it does | Changes files? |
|---|---|---|
| `filescan cleanup-plan` | Lists every file in a folder you marked trash, and every file matched by a delete-candidate rule, in a new plan folder under `cleanup\`. Each file is marked "covered" if an identical copy is kept elsewhere, or "unique". | No |
| `filescan cleanup --plan <folder>` | Moves the plan's files into `_filescan_quarantine` on the same disk. `--dry-run` only counts. | **Moves** (reversible) |
| `filescan cleanup-restore --plan <folder>` | Puts a plan's quarantined files back where they were. | **Moves** |
| `filescan cleanup-purge --plan <folder> --yes` | Deletes a plan's quarantined files for good, once they have been in quarantine for 14 days. Without `--yes` it only reports. A covered file stays in quarantine if its copy has since gone or changed. | **Deletes** |
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

Everything goes in the configured `filescan_folder`, for example `C:\filescan`:

- `file_index.db`: the database, the record of every file, hash and decision;
- `filescan_report.xlsx` and the report CSVs named above;
- `cleanup\<plan id>\`: cleanup plans and their action logs;
- `build\<plan id>\`: build plans;
- `deletions\`: what `delete-folder` deleted;
- `logs\`: files and folders that could not be read.

## Older code

The first prototype and the older merge workflow (`run`, `similarity`, `plan`, `merge-review`, `execute`, the old Excel `report`, `waste`, `largest`, `clusters`, `duplicates` and `hash-server`) are in `unused/`. They are not used, tested or maintained, and can be restored from there if needed. The separate report commands (`exclusions-report`, `unwanted-report`, `measure` and `compare`) are now part of `report`.

## Tests

```
poetry run pytest
```

pytest runs the tests in `tests/`. A test checks that every command this README names exists.
