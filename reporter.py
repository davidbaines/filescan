import yaml
from pathlib import Path
from openpyxl import Workbook
from database import FileDB


class Reporter:
    "Generate xlsx report on duplicate files and similar folders"
    def __init__(self, config_path):
        self.cfg = yaml.safe_load(Path(config_path).read_text())
        self.db = FileDB(self.cfg["database"]["path"])

    def _summary_data(self):
        ex = lambda q: self.db.conn.execute(q).fetchone()[0]
        return [
            ("Total files", ex("SELECT COUNT(*) FROM files")),
            ("Total folders", ex("SELECT COUNT(*) FROM folders")),
            ("Similar folder pairs", ex("SELECT COUNT(*) FROM similarity_results")),
            ("Confirmed duplicate files", ex("SELECT COUNT(*) FROM file_matches WHERE full_hash_match=1")),
            ("Reclaimable GB", round((ex("SELECT COALESCE(SUM(f.size),0) FROM file_matches fm JOIN files f ON fm.file_a_id=f.id WHERE fm.full_hash_match=1") or 0) / 1e9, 2)),
        ]

    def _duplicate_files_data(self):
        hdrs = ["file_a", "file_b", "filename", "size_bytes", "folder_a", "folder_b", "folder_similarity"]
        rows = self.db.conn.execute(
            """SELECT fa.path, fb.path, fa.filename, fa.size, da.path, db.path, sr.jaccard_score
               FROM file_matches fm
               JOIN similarity_results sr ON fm.result_id = sr.id
               JOIN files fa ON fm.file_a_id = fa.id
               JOIN files fb ON fm.file_b_id = fb.id
               JOIN folders da ON fa.folder_id = da.id
               JOIN folders db ON fb.folder_id = db.id
               WHERE fm.full_hash_match = 1
               ORDER BY fa.size DESC""").fetchall()
        return hdrs, rows

    def _similar_folders_data(self):
        hdrs = ["folder_a", "folder_b", "jaccard", "shared_sigs", "files_a", "files_b", "bytes_a", "bytes_b", "confirmed_dups", "dup_bytes"]
        rows = self.db.conn.execute(
            """SELECT da.path, db.path, sr.jaccard_score, sr.shared_count,
                      da.file_count, db.file_count, da.total_bytes, db.total_bytes,
                      (SELECT COUNT(*) FROM file_matches fm WHERE fm.result_id = sr.id AND fm.full_hash_match = 1),
                      (SELECT SUM(fa.size) FROM file_matches fm JOIN files fa ON fm.file_a_id = fa.id
                       WHERE fm.result_id = sr.id AND fm.full_hash_match = 1)
               FROM similarity_results sr
               JOIN folders da ON sr.folder_a_id = da.id
               JOIN folders db ON sr.folder_b_id = db.id
               ORDER BY 10 DESC NULLS LAST""").fetchall()
        return hdrs, rows

    def _write_sheet(self, ws, hdrs, rows):
        ws.append(hdrs)
        for r in rows: ws.append(list(r))

    def run(self, out_path="filescan_report.xlsx"):
        wb = Workbook()

        ws_sum = wb.active
        ws_sum.title = "Summary"
        self._write_sheet(ws_sum, ["Metric", "Value"], self._summary_data())

        hdrs, rows = self._duplicate_files_data()
        self._write_sheet(wb.create_sheet("Duplicate Files"), hdrs, rows)
        total_dup_bytes = sum(r[3] for r in rows)

        hdrs, rows = self._similar_folders_data()
        self._write_sheet(wb.create_sheet("Similar Folders"), hdrs, rows)

        wb.save(out_path)
        print(f"Duplicate files: {len(rows)} folder pairs")
        print(f"Reclaimable: {total_dup_bytes/1e9:.2f} GB")
        print(f"Written to {out_path}")
        self.db.close()
