from collections import defaultdict

from pathlib import Path



import yaml

from database import FileDB





class Analyser:

    "Analyse folder similarity using Jaccard index on tree signatures"



    def __init__(self, config_path):

        self.cfg = yaml.safe_load(Path(config_path).read_text())

        self.db = FileDB(self.cfg["database"]["path"])

        self.threshold = self.cfg["analysis"]["similarity_threshold"]



    def _build_tree_signatures(self):

        """Build {folder_id: set of (filename, size)} including all descendants"""

        folders = self.db.conn.execute("SELECT id, path FROM folders").fetchall()

        folder_paths = {fid: fpath for fid, fpath in folders}

        signatures = {}

        for fid, fpath in folders:

            # Match this folder and all subfolders, avoiding partial name matches

            # e.g. /docs must not match /docs2

            files = self.db.conn.execute(

                """SELECT f.filename, f.size FROM files f

                   JOIN folders d ON f.folder_id = d.id

                   WHERE d.path = ? OR d.path LIKE ? || '/%'""",

                (fpath, fpath.rstrip("/")),

            ).fetchall()

            if files:

                signatures[fid] = set(files)

        return signatures, folder_paths



    def _is_ancestor(self, path_a, path_b):

        """Check if either path is an ancestor of the other"""

        a = path_a.rstrip("/") + "/"

        b = path_b.rstrip("/") + "/"

        return a.startswith(b) or b.startswith(a)



    def _find_candidates(self, sigs, dirty_ids, max_common=100):
        "Use inverted index to find pairs where at least one folder is dirty"
        inverted = defaultdict(set)
        for fid, s in sigs.items():
            for sig in s: inverted[sig].add(fid)

        skipped = sum(1 for fids in inverted.values() if len(fids) > max_common)
        print(f"  Skipping {skipped} overly common signatures (>{max_common} folders)")

        candidates = set()
        for folder_ids in inverted.values():
            if len(folder_ids) > max_common: continue
            ids = sorted(folder_ids)
            for i, a in enumerate(ids):
                for b in ids[i+1:]:
                    if a in dirty_ids or b in dirty_ids: candidates.add((a, b))
        return candidates



    def _jaccard(self, set_a, set_b):

        intersection = len(set_a & set_b)

        union = len(set_a | set_b)

        return intersection / union if union else 0.0



    def analyse(self):
        "Run incremental similarity analysis on new/changed folders"
        dirty_ids = self.db.get_dirty_folder_ids()
        if not dirty_ids:
            print("No new or changed folders to analyse.")
            self.db.close()
            return

        print(f"Found {len(dirty_ids)} new/changed folders to analyse")
        print("Building tree signatures...")
        sigs, folders = self._build_tree_signatures()
        print(f"Found {len(sigs)} folders with files")

        # Build ancestor pairs for filtering
        path_to_id = {v: k for k, v in folders.items()}
        ancestor_pairs = set()
        for fid, fp in folders.items():
            p = str(Path(fp).parent)
            while p in path_to_id:
                a, b = min(fid, path_to_id[p]), max(fid, path_to_id[p])
                ancestor_pairs.add((a, b))
                nxt = str(Path(p).parent)
                if nxt == p: break
                p = nxt

        print("Finding candidates...")
        candidates = self._find_candidates(sigs, dirty_ids)
        print(f"Evaluating {len(candidates)} candidate pairs...")

        sig_lens = {fid: len(s) for fid, s in sigs.items()}
        results, skipped_ancestor, skipped_size = [], 0, 0
        for a, b in candidates:
            pair = (min(a, b), max(a, b))
            if pair in ancestor_pairs:
                skipped_ancestor += 1
                continue
            la, lb = sig_lens[a], sig_lens[b]
            if min(la, lb) / max(la, lb) < self.threshold:
                skipped_size += 1
                continue
            inter = len(sigs[a] & sigs[b])
            union = la + lb - inter
            score = inter / union if union else 0.0
            if score >= self.threshold: results.append((a, b, score, inter))

        print(f"  Skipped {skipped_ancestor} ancestor pairs, {skipped_size} by size pre-filter")

        # Remove old results involving dirty folders, then insert new
        self.db.conn.execute(
            "DELETE FROM similarity_results WHERE folder_a_id IN ({seq}) OR folder_b_id IN ({seq})".format(
                seq=",".join(str(d) for d in dirty_ids)))
        for a, b, score, shared in results:
            self.db.conn.execute(
                """INSERT OR REPLACE INTO similarity_results
                   (folder_a_id, folder_b_id, jaccard_score, shared_count, threshold_used, created_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                (a, b, score, shared, self.threshold))
        self.db.commit()
        self.db.mark_analysed(dirty_ids)

        print(f"\nFound {len(results)} similar folder pairs (threshold >= {self.threshold})")
        for a, b, score, shared in sorted(results, key=lambda x: -x[2]):
            print(f"  {folders[a]}")
            print(f"  {folders[b]}")
            print(f"  Jaccard: {score:.3f}, Shared: {shared} files\n")

        self.db.close()

