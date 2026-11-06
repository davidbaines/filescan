from __future__ import annotations

from pathlib import Path

from filescan.progress import SkipLog, progress_bar


def test_skip_log_counts_by_reason_writes_a_file_and_prints_only_a_summary(tmp_path: Path, capsys) -> None:
    log = SkipLog(tmp_path, "test")
    with progress_bar(desc="work", total=3, unit="file") as bar:
        log.attach(bar)
        log.add("quick hash", r"E:\a.bin", PermissionError(13, "Permission denied"))
        log.add("quick hash", r"E:\b.bin", PermissionError(13, "Permission denied"))
        log.add("folder", r"E:\c", FileNotFoundError(2, "gone"))
        assert "skipped 3" in str(bar)
    log.summary()
    out = capsys.readouterr()
    assert r"E:\a.bin" not in out.out and r"E:\a.bin" not in out.err  # no line per file on screen
    assert "Skipped 3:" in out.out
    assert "2  quick hash: PermissionError" in out.out
    lines = log.path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3 and lines[0].startswith("quick hash\tE:\\a.bin\t")


def test_skip_log_writes_nothing_when_there_is_nothing_to_report(tmp_path: Path, capsys) -> None:
    log = SkipLog(tmp_path, "test")
    log.summary()
    assert not (tmp_path / "logs").exists()
    assert capsys.readouterr().out == ""
