import os
import time

import pytest

from ptool.analyse import compare, compare_compact, directory_map, merge, read_csv
from ptool.checksums import stats


def _make_csv(tmp_path, name, files):
    """Write a snapshot CSV from a dict of {filename: content_bytes}."""
    records = ["checksum,fsize,mtime,fpath"]
    for fname, content in files.items():
        f = tmp_path / fname
        f.write_bytes(content)
        r = stats(str(f))
        assert not r.has_error(), f"stats failed for {fname}: {r.result()}"
        records.append(r.value)
    csv_path = tmp_path / f"{name}.csv"
    csv_path.write_text("\n".join(records) + "\n")
    return str(csv_path)


@pytest.fixture
def two_identical_pools(tmp_path):
    left_dir = tmp_path / "left"
    right_dir = tmp_path / "right"
    left_dir.mkdir()
    right_dir.mkdir()
    content = {"data.nc": b"\xAB" * 1024}
    left_csv = _make_csv(left_dir, "snapshot_left", content)
    right_csv = _make_csv(right_dir, "snapshot_right", content)
    return left_csv, right_csv


@pytest.fixture
def one_unique_pool(tmp_path):
    left_dir = tmp_path / "left"
    right_dir = tmp_path / "right"
    left_dir.mkdir()
    right_dir.mkdir()
    left_csv = _make_csv(left_dir, "snapshot_left", {"only_left.nc": b"\xAA" * 1024, "common.nc": b"\xBB" * 1024})
    right_csv = _make_csv(right_dir, "snapshot_right", {"common.nc": b"\xBB" * 1024})
    return left_csv, right_csv


class TestReadCsv:
    def test_loads_records(self, tmp_path):
        d = tmp_path / "pool"
        d.mkdir()
        csv = _make_csv(d, "snap", {"a.nc": b"\x00" * 512})
        df, dups = read_csv(csv)
        assert len(df) == 1
        assert "checksum" in df.columns
        assert "fsize" in df.columns
        assert "mtime" in df.columns
        assert "fpath" in df.columns

    def test_derives_fname_and_rpath(self, tmp_path):
        d = tmp_path / "pool"
        d.mkdir()
        csv = _make_csv(d, "snap", {"file.nc": b"\x00" * 256})
        df, _ = read_csv(csv)
        assert df["fname"].iloc[0] == "file.nc"
        assert "rpath" in df.columns

    def test_no_duplicates_when_files_unique(self, tmp_path):
        d = tmp_path / "pool"
        d.mkdir()
        csv = _make_csv(d, "snap", {"a.nc": b"\xAA" * 512, "b.nc": b"\xBB" * 512})
        df, dups = read_csv(csv)
        assert len(df) == 2
        assert len(dups) == 0

    def test_filters_dash_checksums(self, tmp_path):
        # read_csv filters rows where checksum == "-", but a CSV with only
        # such rows leaves an empty DataFrame and commonpath([]) raises.
        # Mixed CSVs (some valid rows) work fine.
        d = tmp_path / "pool"
        d.mkdir()
        csv = _make_csv(d, "snap", {"a.nc": b"\x00" * 256})
        # Manually append a dash row to the valid CSV
        with open(csv, "a") as f:
            f.write("-,0,0.0,/some/phantom\n")
        df, _ = read_csv(csv)
        # The dash row is filtered out; only the real file remains
        assert len(df) == 1
        assert all(df["checksum"] != "-")


class TestCompare:
    def test_identical_files_classified_correctly(self, two_identical_pools):
        left_csv, right_csv = two_identical_pools
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        result = compare(left, right)
        assert "identical" in result.index

    def test_unique_files_classified_correctly(self, one_unique_pool):
        left_csv, right_csv = one_unique_pool
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        result = compare(left, right)
        assert "unique" in result.index
        assert "identical" in result.index

    def test_renamed_files_classified_correctly(self, tmp_path):
        left_dir = tmp_path / "left"
        right_dir = tmp_path / "right"
        left_dir.mkdir()
        right_dir.mkdir()
        content = b"\xCC" * 1024
        left_csv = _make_csv(left_dir, "snap_left", {"original.nc": content})
        right_csv = _make_csv(right_dir, "snap_right", {"renamed.nc": content})
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        result = compare(left, right)
        assert "renamed" in result.index

    def test_empty_result_when_no_overlap(self, tmp_path):
        left_dir = tmp_path / "left"
        right_dir = tmp_path / "right"
        left_dir.mkdir()
        right_dir.mkdir()
        left_csv = _make_csv(left_dir, "snap_left", {"left_only.nc": b"\xAA" * 512})
        right_csv = _make_csv(right_dir, "snap_right", {"right_only.nc": b"\xBB" * 512})
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        result = compare(left, right)
        assert "identical" not in result.index
        assert "unique" in result.index


class TestCompareCompact:
    def test_returns_rpath_columns(self, two_identical_pools):
        left_csv, right_csv = two_identical_pools
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        result = compare_compact(left, right)
        assert "rpath_left" in result.columns
        assert "rpath_right" in result.columns


class TestMergeAndDirectoryMap:
    def test_merge_on_checksum(self, two_identical_pools):
        left_csv, right_csv = two_identical_pools
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        m = merge(left, right)
        assert len(m) == 1

    def test_directory_map_returns_pairs(self, two_identical_pools):
        left_csv, right_csv = two_identical_pools
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        m = merge(left, right)
        dm = directory_map(m)
        assert "rparent_left" in dm.columns
        assert "rparent_right" in dm.columns
