from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import yaml
from tqdm import tqdm
from database import FileDB

class Scanner:
    "Scan drives for files and index them into the database"

    def __init__(self, config_path):
        self.cfg = yaml.safe_load(Path(config_path).read_text())
        self.db = FileDB(self.cfg["database"]["path"])
        self.filters = self.cfg.get("scan_filters", {})

    def _excluded_folder(self, p):
        return p.name in self.filters.get("exclude_folders", [])

    def _excluded_file(self, p):
        if p.suffix in self.filters.get("exclude_extensions", []):
            return True
        sz = p.stat().st_size
        mn, mx = self.filters.get("min_file_size", 0), self.filters.get("max_file_size")
        if sz < mn:
            return True
        if mx and sz > mx:
            return True
        return False

    def _scan_folder(self, folder, drive):
        try:
            entries = list(folder.iterdir())
        except PermissionError:
            return [], []
        files, subdirs = [], []
        for p in entries:
            if p.is_dir() and not self._excluded_folder(p):
                subdirs.append(p)

            elif p.is_file() and not self._excluded_file(p):
                files.append(p)
        return files, subdirs

    def _index_folder(self, folder, drive, depth):
        files, subdirs = self._scan_folder(folder, drive)
        total_bytes = sum(f.stat().st_size for f in files)
        self.db.upsert_folder(str(folder), drive, len(files), total_bytes, depth)
        self.db.commit()
        fid = self.db.get_folder_id(str(folder))
        for f in files:
            st = f.stat()
            if self.db.needs_rescan(str(f), st.st_mtime):
                self.db.upsert_file(
                    fid, f.name, str(f), st.st_size, st.st_mtime, st.st_ctime
                )

        self.db.commit()
        return subdirs

    def scan(self):
        "Scan the top level folders specified using threaded IO"

        for top_folder in self.cfg["folders"]:
            root = Path(top_folder)
            if not root.is_dir():
                print(f"Skipping {top_folder}: not found")
                continue

            print(f"Scanning {top_folder}")
            queue = [(root, 0)]
            pbar = tqdm(desc="  Folders", unit="dir")
            with ThreadPoolExecutor(max_workers=8) as pool:
                while queue:
                    futures = {
                        pool.submit(self._index_folder, folder, top_folder, depth): (folder, depth)
                        for folder, depth in queue
                    }
                    queue = []
                    for fut in as_completed(futures):
                        pbar.update(1)
                        subdirs = fut.result()
                        parent_depth = futures[fut][1]
                        queue.extend((sd, parent_depth + 1) for sd in subdirs)
            pbar.close()
            print(f"Done scanning {top_folder}")
        self.db.close()
