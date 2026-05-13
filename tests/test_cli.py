import os

import pytest
from click.testing import CliRunner

from ptool.cli import cli
from ptool.checksums import stats


def _make_csv(tmp_path, name, files):
    records = ["checksum,fsize,mtime,fpath"]
    for fname, content in files.items():
        f = tmp_path / fname
        f.write_bytes(content)
        r = stats(str(f))
        assert not r.has_error()
        records.append(r.value)
    csv_path = tmp_path / f"{name}.csv"
    csv_path.write_text("\n".join(records) + "\n")
    return str(csv_path)


@pytest.fixture
def runner():
    return CliRunner()


class TestChecksumsCommand:
    def test_runs_on_directory(self, runner, tmp_path):
        (tmp_path / "a.nc").write_bytes(b"\x00" * 512)
        result = runner.invoke(cli, ["checksums", str(tmp_path)])
        assert result.exit_code == 0
        assert "imohash:" in result.output

    def test_runs_on_single_file(self, runner, tmp_path):
        f = tmp_path / "data.nc"
        f.write_bytes(b"\x00" * 512)
        result = runner.invoke(cli, ["checksums", str(f)])
        assert result.exit_code == 0
        assert "imohash:" in result.output

    def test_output_has_csv_header(self, runner, tmp_path):
        (tmp_path / "a.nc").write_bytes(b"\x00" * 256)
        result = runner.invoke(cli, ["checksums", str(tmp_path)])
        assert "checksum,fsize,mtime,fpath" in result.output

    def test_ignore_flag(self, runner, tmp_path):
        (tmp_path / "keep.nc").write_bytes(b"\x00" * 256)
        (tmp_path / "skip.tmp").write_bytes(b"\x00" * 256)
        result = runner.invoke(cli, ["checksums", "--ignore", "*.tmp", str(tmp_path)])
        assert result.exit_code == 0
        assert "skip.tmp" not in result.output
        assert "keep.nc" in result.output

    def test_drop_hidden_files_default(self, runner, tmp_path):
        (tmp_path / "visible.nc").write_bytes(b"\x00" * 256)
        (tmp_path / ".hidden").write_bytes(b"\x00" * 256)
        result = runner.invoke(cli, ["checksums", str(tmp_path)])
        assert result.exit_code == 0
        assert ".hidden" not in result.output

    def test_no_drop_hidden_files(self, runner, tmp_path):
        (tmp_path / "visible.nc").write_bytes(b"\x00" * 256)
        (tmp_path / ".hidden").write_bytes(b"\x00" * 256)
        result = runner.invoke(cli, ["checksums", "--no-drop-hidden-files", str(tmp_path)])
        assert result.exit_code == 0
        assert ".hidden" in result.output

    def test_output_to_file(self, runner, tmp_path):
        (tmp_path / "a.nc").write_bytes(b"\x00" * 256)
        out = tmp_path / "out.csv"
        result = runner.invoke(cli, ["checksums", "-o", str(out), str(tmp_path)])
        assert result.exit_code == 0
        assert out.exists()
        assert "imohash:" in out.read_text()


class TestCompareCommand:
    def test_compare_identical_pools(self, runner, tmp_path):
        left_dir = tmp_path / "left"
        right_dir = tmp_path / "right"
        left_dir.mkdir()
        right_dir.mkdir()
        content = {"data.nc": b"\xAB" * 1024}
        left_csv = _make_csv(left_dir, "left", content)
        right_csv = _make_csv(right_dir, "right", content)
        result = runner.invoke(cli, ["compare", left_csv, right_csv])
        assert result.exit_code == 0

    def test_compare_shows_unique(self, runner, tmp_path):
        left_dir = tmp_path / "left"
        right_dir = tmp_path / "right"
        left_dir.mkdir()
        right_dir.mkdir()
        left_csv = _make_csv(left_dir, "left", {"common.nc": b"\xAA" * 512, "extra.nc": b"\xBB" * 512})
        right_csv = _make_csv(right_dir, "right", {"common.nc": b"\xAA" * 512})
        result = runner.invoke(cli, ["compare", left_csv, right_csv])
        assert result.exit_code == 0


class TestCliHelp:
    def test_root_help(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "checksums" in result.output
        assert "compare" in result.output
        assert "summary" in result.output

    def test_checksums_help(self, runner):
        result = runner.invoke(cli, ["checksums", "--help"])
        assert result.exit_code == 0
        assert "--ignore" in result.output
        assert "--outfile" in result.output

    def test_compare_help(self, runner):
        result = runner.invoke(cli, ["compare", "--help"])
        assert result.exit_code == 0

    def test_summary_help(self, runner):
        result = runner.invoke(cli, ["summary", "--help"])
        assert result.exit_code == 0

    def test_prepare_rsync_help(self, runner):
        result = runner.invoke(cli, ["prepare-rsync", "--help"])
        assert result.exit_code == 0
