from __future__ import annotations

from pathlib import Path

from filescan.dedupe.duplicates import DuplicateDetector
from filescan.models import ScanConfig
from filescan.similarity.folders import FolderSimilarityAnalyser
from tests.helpers.indexing import index_paths
from tests.helpers.tree_builder import build_tree


def test_similarity_excludes_ancestor_pairs_and_ranks_backup_folder(tmp_path: Path, make_repo) -> None:
    root = tmp_path / "root"
    created = build_tree(
        root,
        {
            "project/sub/shared.txt": b"same-data-1234",
            "project/readme.txt": b"project-readme",
            "project_backup/sub/shared.txt": b"same-data-1234",
            "project_backup/readme.txt": b"project-readme",
            "other/random.txt": b"completely-different",
        },
    )
    db, repo = make_repo(tmp_path / "similarity.db")
    index_paths(repo, root, created)
    config = ScanConfig(
        roots=[root],
        filescan_folder=tmp_path / "filescan",
        database_path=tmp_path / "similarity.db",
        report_path=tmp_path / "report.xlsx",
        duplicate_size_threshold=8,
        similarity_threshold=0.2,
        merge_threshold=0.9,
    )
    DuplicateDetector(config).run()

    candidates = FolderSimilarityAnalyser(config).run()

    candidate_pairs = {frozenset((candidate.folder_a, candidate.folder_b)) for candidate in candidates}
    assert frozenset((root / "project", root / "project" / "sub")) not in candidate_pairs
    assert any({"project", "project_backup"} == {candidate.folder_a.name, candidate.folder_b.name} for candidate in candidates)
    backup_candidate = next(
        candidate for candidate in candidates if {candidate.folder_a.name, candidate.folder_b.name} == {"project", "project_backup"}
    )
    assert backup_candidate.name_bonus > 0
    db.close()
