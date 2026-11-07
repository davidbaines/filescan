This folder contains a persistent synthetic dataset and generated outputs for
manual verification of the XLSX report.

Expected workflow:
- The integration test reads files under `input/`.
- It writes config, SQLite, JSON artifacts, and `filescan_report.xlsx` under
  `output/`.
- The test intentionally does not clean the generated files so they can be
  inspected manually after the test run.
