from collections import defaultdict
from pathlib import Path
import yaml
from tqdm import tqdm
from database import FileDB


class Analyser:
    "Analyse folder similarity using Jaccard index on tree signatures"

    def __init__(self, config_path):
        self.cfg = yaml.safe_load(Path(config_path).read_text())
        self.db = FileDB(self.cfg["database"]["path"])
        self.threshold = self.cfg["analysis"]["similarity_threshold"]

    def _build_tree_signatures(self):
        folders = dict(self.db.conn.execute("SELECT id, path FROM folders").fetchall())
        path_to_id = {v: k for k, v in folders.items()}
        rows = self.db.conn.execute(
            "SELECT d.path, f.filename, f.size FROM files f JOIN folders d ON f.folder_id = d.id").fetchall()
        print(f"  Loaded {len(rows)} file records, {len(folders)} folders")

        ancestors = defaultdict(list)
        for fp in tqdm(path_to_id, desc="  Building ancestry"):
            p = str(Path(fp).parent)
            while p in path_to_id:
                ancestors[path_to_id[fp]].append(path_to_id[p])
                nxt = str(Path(p).parent)
                if nxt == p: break
                p = nxt

        direct = defaultdict(set)
        for fp, fn, sz in rows: direct[path_to_id[fp]].add(hash((fn, sz)))

        sigs = defaultdict(set)
        for fid, s in tqdm(direct.items(), desc="  Propagating to ancestors"):
            sigs[fid] |= s
            for aid in ancestors[fid]: sigs[aid] |= s

        return sigs, folders, ancestors

    def _build_ancestor_pairs(self, ancestors):
        pairs = set()
        for fid, aids in ancestors.items():
            for aid in aids:
                pairs.add((min(fid, aid), max(fid, aid)))
        return pairs

    def _find_candidates(self, sigs, max_common=100):
        inverted = defaultdict(set)
        for fid, s in sigs.items():
            for sig in s: inverted[sig].add(fid)

        skipped = sum(1 for fids in inverted.values() if len(fids) > max_common)
        print(f"  Skipping {skipped} overly common signatures (>{max_common} folders)")

        candidates = set()
        for folder_ids in tqdm(inverted.values(), desc="  Building candidate pairs"):
            if len(folder_ids) > max_common: continue
            ids = sorted(folder_ids)
            for i, a in enumerate(ids):
                for b in ids[i+1:]: candidates.add((a, b))
        return candidates

    def analyse(self):
        print("Building tree signatures...")
        sigs, folders, ancestors = self._build_tree_signatures()
        print(f"Found {len(sigs)} folders with files")

        print("Precomputing ancestor pairs...")
        ancestor_pairs = self._build_ancestor_pairs(ancestors)

        print("Finding candidates...")
        candidates = self._find_candidates(sigs)
        print(f"Evaluating {len(candidates)} candidate pairs...")

        sig_lens = {fid: len(s) for fid, s in sigs.items()}
        results = []
        skipped_ancestor, skipped_size = 0, 0
        for a, b in tqdm(candidates, desc="  Computing Jaccard"):
            if (a, b) in ancestor_pairs or (b, a) in ancestor_pairs:
                skipped_ancestor += 1
                continue
            la, lb = sig_lens[a], sig_lens[b]
            if min(la, lb) / max(la, lb) < self.threshold:
                skipped_size += 1
                continue
            inter = len(sigs[a] & sigs[b])
            union = la + lb - inter
            score = inter / union if union else 0.0
            if score < self.threshold: continue
            results.append((a, b, score, inter))

        print(f"  Skipped {skipped_ancestor} ancestor pairs, {skipped_size} by size pre-filter")

        self.db.conn.execute("DELETE FROM similarity_results")
        for a, b, score, shared in results:
            self.db.conn.execute(
                """INSERT INTO similarity_results
                   (folder_a_id, folder_b_id, jaccard_score, shared_count, threshold_used, created_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                (a, b, score, shared, self.threshold))
        self.db.commit()

        print(f"\nFound {len(results)} similar folder pairs (threshold >= {self.threshold})")
        # for a, b, score, shared in sorted(results, key=lambda x: -x[2]):
        #     print(f"  {folders[a]}")
        #     print(f"  {folders[b]}")
        #     print(f"  Jaccard: {score:.3f}, Shared: {shared} files\n")

        self.db.close()
