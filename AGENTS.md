# AGENTS.md

## Project Intent
This project organizes files and folders on Windows systems using a review-first workflow.

Primary workflow:
`scan -> duplicates -> similarity -> plan -> report -> execute (explicit only)`

## Product Rules
- Windows only.
- Use `pathlib.Path` instead of string paths where possible.
- Use `pathlib.Path` instead of the os module where possible.
- Suggestions must be user-reviewed before any file operations.
- Do not auto-delete source data in the normal pipeline.
- Treat exact duplicate files and similar folders as separate concerns.

## Analysis Rules
- Exact duplicate file detection should be independent from folder similarity detection.
- Folder similarity should use multiple heuristics, not Jaccard alone.
- Backup-like folder names include: `backup`, `copy`, `old`, `temp`, `temporary`.
- Categorization suggestions may use metadata/content analysis, but final decisions are user-curated.

## UX Rules
- Always show the currently active process during long-running work.
- Any long-running iterative process should use a visible `tqdm` progress bar.
- Progress bars should remain visible after completion.
- The default CLI command should run the normal non-destructive pipeline.
- The default pipeline should produce a final summary and a report in the configured filescan folder.

## Config Rules
- Use a shared `filescan_folder` for related outputs.
- Read database and report locations from:
  - `database_folder`
  - `database_filename`
  - `report_filename`

## Engineering Rules
- Follow TDD with `pytest`.
- Keep startup checks fast and fail early if the configured database is not writable.
- Make the code restartable, so that if it crashes most of the work is saved in the database.
- Don't rescan or re-analyse previously scanned folders unless a --rescan option is given.
- Preserve safety and observability over raw speed.
- Catch PermissionErrors and other disk operation errors, ignore those files and continue.
