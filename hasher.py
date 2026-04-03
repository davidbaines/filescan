import yaml, xxhash
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from database import FileDB

CHUNK = 4096

class Hasher:
    "Two-stage file hashing for duplicate confirmation"
    def __init__(self, config_path):
        self.cfg = yaml.safe_load(Path(config_path).read_text())
        self.db = FileDB(self.cfg["database"]["path"])

    def _quick_hash(self, path):
        try:
            p = Path(path)
            sz = p.stat().st_size
            with open(p, "rb") as f:
                head = f.read(CHUNK)
                if sz > CHUNK:
                    f.seek(max(0, sz - CHUNK))
                    tail = f.read(CHUNK)
                else: tail = b""
            return xxhash.xxh128(head + tail + str(sz).encode()).hexdigest()
        except (OSError, PermissionError): return None

    def _full_hash(self, path):
        try:
            h = xxhash.xxh128()
            with open(path, "rb") as f:
                while chunk := f.read(1 << 20): h.update(chunk)
            return h.hexdigest()
        except (OSError, PermissionError): return None

    def _get_or_compute_hash(self, file_id, path, full=False):
        existing = self.db.get_hash(file_id)
        if existing:
            qh, fh = existing
            if not full and qh: return qh
            if full and fh: return fh
        if full:
            h = self._full_hash(path)
            if h: self.db.upsert_hash(file_id, full_hash=h)
        else:
            h = self._quick_hash(path)
            if h: self.db.upsert_hash(file_id, quick_hash=h)
        return h

    def run(self):
        "Run two-stage hashing on candidate file pairs"
        pairs = self.db.get_candidate_file_pairs()
        if not pairs:
            print("No candidate file pairs to hash.")
            self.db.close()
            return

        print(f"Stage 1: Quick hashing {len(pairs)} candidate pairs...")
        quick_matches = []
        for result_id, fa_id, fb_id, fa_path, fb_path, fname, fsize in tqdm(pairs, desc="  Quick hash"):
            ha = self._get_or_compute_hash(fa_id, fa_path)
            hb = self._get_or_compute_hash(fb_id, fb_path)
            if ha is None or hb is None:
                self.db.insert_file_match(result_id, fa_id, fb_id, False, None)
                continue
            if ha == hb: quick_matches.append((result_id, fa_id, fb_id, fa_path, fb_path, fname, fsize))
            else: self.db.insert_file_match(result_id, fa_id, fb_id, False, None)
        self.db.commit()

        print(f"\nStage 1 result: {len(quick_matches)}/{len(pairs)} pairs match on quick hash")
        if not quick_matches:
            self.db.close()
            return

        print(f"\nStage 2: Full hashing {len(quick_matches)} pairs...")
        confirmed, rejected = 0, 0
        for result_id, fa_id, fb_id, fa_path, fb_path, fname, fsize in tqdm(quick_matches, desc="  Full hash"):
            ha = self._get_or_compute_hash(fa_id, fa_path, full=True)
            hb = self._get_or_compute_hash(fb_id, fb_path, full=True)
            if ha and hb and ha == hb:
                self.db.insert_file_match(result_id, fa_id, fb_id, True, True)
                confirmed += 1
            else:
                self.db.insert_file_match(result_id, fa_id, fb_id, True, False)
                rejected += 1
        self.db.commit()

        print(f"\nStage 2 result: {confirmed} confirmed duplicates, {rejected} false positives")
        self.db.close()
