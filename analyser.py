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



    def _find_candidates(self, signatures):

        """Use inverted index to find folder pairs sharing at least one file signature"""

        inverted = defaultdict(set)

        for fid, sigs in signatures.items():

            for sig in sigs:

                inverted[sig].add(fid)



        candidates = set()

        for folder_ids in inverted.values():

            if len(folder_ids) > 1:

                ids = sorted(folder_ids)

                for i, a in enumerate(ids):

                    for b in ids[i + 1 :]:

                        candidates.add((a, b))

        return candidates



    def _jaccard(self, set_a, set_b):

        intersection = len(set_a & set_b)

        union = len(set_a | set_b)

        return intersection / union if union else 0.0



    def analyse(self):

        """Run similarity analysis and store results in DB"""

        print("Building tree signatures...")

        signatures, folder_paths = self._build_tree_signatures()

        print(f"Found {len(signatures)} folders with files")



        candidates = self._find_candidates(signatures)

        print(f"Evaluating {len(candidates)} candidate pairs...")



        results = []

        for a, b in candidates:

            if self._is_ancestor(folder_paths[a], folder_paths[b]):

                continue

            score = self._jaccard(signatures[a], signatures[b])

            shared = len(signatures[a] & signatures[b])

            if score >= self.threshold:

                results.append((a, b, score, shared))



        # Clear previous results and write new

        self.db.conn.execute("DELETE FROM similarity_results")

        for a, b, score, shared in results:

            self.db.conn.execute(

                """INSERT INTO similarity_results

                   (folder_a_id, folder_b_id, jaccard_score, shared_count, threshold_used, created_at)

                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",

                (a, b, score, shared, self.threshold),

            )

        self.db.commit()



        # Display results

        print(f"\nFound {len(results)} similar folder pairs (threshold >= {self.threshold})")

        for a, b, score, shared in sorted(results, key=lambda x: -x[2]):

            print(f"  {folder_paths[a]}")

            print(f"  {folder_paths[b]}")

            print(f"  Jaccard: {score:.3f}, Shared: {shared} files\n")



        self.db.close()

