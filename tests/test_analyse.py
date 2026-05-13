import os
import time

import pytest

from ptool.analyse import _suspicious_pairs, compare, compare_compact, directory_map, merge, read_csv
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


class TestTFIDFFolderMapping:
    """TF-IDF weighted association should prefer specific (rare) filenames over
    generic (ubiquitous) ones when mapping directories across sites.

    Scenario mirroring the real-world fArc_sorted false mapping:
      Left side has three directories that all share three generic files
      (IDF ≈ 0). dir_a also has one specific file (IDF = log 3 ≈ 1.10).
      Right side has:
        dir_x — specific_a + 1 generic  (2 raw matches with dir_a)
        dir_y — 3 generics              (3 raw matches with dir_a — wins on raw count)
      Raw count would map dir_a → dir_y (wrong).
      TF-IDF maps dir_a → dir_x (correct).
    """

    def _build_pool(self, base, subdirs, files_per_subdir):
        """Create directory tree and return {relative_path: content} dict."""
        for d in subdirs:
            (base / d).mkdir(parents=True, exist_ok=True)
        result = {}
        for subdir, files in files_per_subdir.items():
            for fname, content in files.items():
                result[f"{subdir}/{fname}"] = content
        return result

    def test_tfidf_prefers_specific_over_generic(self, tmp_path):
        generic  = b"\xAA" * 512
        generic2 = b"\xBB" * 512
        generic3 = b"\xCC" * 512
        specific = b"\xDD" * 512

        left_base = tmp_path / "left"
        for d in ["dir_a", "dir_b", "dir_c"]:
            (left_base / d).mkdir(parents=True)

        # dir_a: 3 generic files (appear in all 3 left dirs) + 1 specific file
        # dir_b, dir_c: only the 3 generic files → IDF of generics = log(3/3) = 0
        left_files = self._build_pool(left_base, [], {
            "dir_a": {"generic.nc": generic, "generic2.nc": generic2,
                      "generic3.nc": generic3, "specific_a.nc": specific},
            "dir_b": {"generic.nc": generic, "generic2.nc": generic2, "generic3.nc": generic3},
            "dir_c": {"generic.nc": generic, "generic2.nc": generic2, "generic3.nc": generic3},
        })
        left_csv = _make_csv(left_base, "left", left_files)

        right_base = tmp_path / "right"
        for d in ["dir_x", "dir_y"]:
            (right_base / d).mkdir(parents=True)

        # dir_x: specific_a + 1 generic → 2 raw matches with dir_a
        # dir_y: all 3 generics        → 3 raw matches with dir_a (raw count picks this — wrong)
        right_files = self._build_pool(right_base, [], {
            "dir_x": {"specific_a.nc": specific, "generic.nc": generic},
            "dir_y": {"generic.nc": generic, "generic2.nc": generic2, "generic3.nc": generic3},
        })
        right_csv = _make_csv(right_base, "right", right_files)

        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        m = merge(left, right)
        dm = directory_map(m)

        # dir_a must map to dir_x (driven by specific_a), not dir_y (more generic matches)
        dir_a_row = dm[dm.rparent_left.str.endswith("dir_a")]
        assert len(dir_a_row) == 1, "dir_a should have exactly one mapping"
        assert dir_a_row.rparent_right.iloc[0].endswith("dir_x"), (
            f"dir_a mapped to {dir_a_row.rparent_right.iloc[0]!r} — "
            "expected dir_x (specific file); raw-count would pick dir_y"
        )

    def test_unique_files_still_map_correctly(self, tmp_path):
        """When all filenames are unique (no generics), TF-IDF behaves like raw count."""
        left_base = tmp_path / "left"
        right_base = tmp_path / "right"
        (left_base / "pool").mkdir(parents=True)
        (right_base / "pool").mkdir(parents=True)

        content = b"\xEE" * 512
        left_csv = _make_csv(left_base, "left", {"pool/data.nc": content})
        right_csv = _make_csv(right_base, "right", {"pool/data.nc": content})

        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        m = merge(left, right)
        assert len(m) == 1


def _make_csv_explicit(tmp_path, name, entries):
    """Write a snapshot CSV with hand-crafted (checksum, fsize, mtime, fpath) rows.

    Bypasses the filesystem so mtime can be controlled precisely — useful for
    tests that need to distinguish modified_latest_left from modified_latest_right.
    """
    records = ["checksum,fsize,mtime,fpath"]
    for checksum, fsize, mtime, fpath in entries:
        records.append(f"{checksum},{fsize},{mtime},{fpath}")
    csv_path = tmp_path / f"{name}.csv"
    csv_path.write_text("\n".join(records) + "\n")
    return str(csv_path)


class TestSuspiciousPairs:
    """_suspicious_pairs() should flag folder mappings where every paired file
    is modified and none are identical — the signature of a false association."""

    def test_all_modified_flagged(self, tmp_path):
        """Two dirs sharing filenames but different content → suspicious."""
        left_csv = _make_csv_explicit(tmp_path, "left", [
            ("imohash:aaa", 512, 1000.0, "/pool/left/dir_a/file1.nc"),
            ("imohash:bbb", 512, 1000.0, "/pool/left/dir_a/file2.nc"),
            ("imohash:eee", 512, 1000.0, "/pool/left/dir_b/other.nc"),
        ])
        right_csv = _make_csv_explicit(tmp_path, "right", [
            ("imohash:ccc", 512, 2000.0, "/pool/right/dir_x/file1.nc"),
            ("imohash:ddd", 512, 2000.0, "/pool/right/dir_x/file2.nc"),
            ("imohash:eee", 512, 1000.0, "/pool/right/dir_y/other.nc"),
        ])
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        cmp = compare(left, right)

        suspicious = _suspicious_pairs(cmp)
        assert len(suspicious) == 1
        left_dirs = {pair[0] for pair in suspicious}
        assert any(d.endswith("dir_a") for d in left_dirs)

    def test_mix_identical_and_modified_not_flagged(self, tmp_path):
        """A pair with at least one identical file should never be flagged."""
        shared_cs = "imohash:shared00"
        left_csv = _make_csv_explicit(tmp_path, "left", [
            (shared_cs,    512, 1000.0, "/pool/left/dir_a/shared.nc"),
            ("imohash:aaa", 512, 1000.0, "/pool/left/dir_a/changed.nc"),
        ])
        right_csv = _make_csv_explicit(tmp_path, "right", [
            (shared_cs,    512, 1000.0, "/pool/right/dir_x/shared.nc"),
            ("imohash:bbb", 512, 2000.0, "/pool/right/dir_x/changed.nc"),
        ])
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        cmp = compare(left, right)

        suspicious = _suspicious_pairs(cmp)
        assert len(suspicious) == 0, (
            "dir_a/dir_x has an identical file — should not be flagged as suspicious"
        )

    def test_unique_only_dir_not_flagged(self, tmp_path):
        """A left directory with no right counterpart (all unique) should not be flagged."""
        left_csv = _make_csv_explicit(tmp_path, "left", [
            ("imohash:aaa", 512, 1000.0, "/pool/left/dir_a/only_left.nc"),
        ])
        right_csv = _make_csv_explicit(tmp_path, "right", [
            ("imohash:bbb", 512, 1000.0, "/pool/right/dir_x/only_right.nc"),
        ])
        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)
        cmp = compare(left, right)

        suspicious = _suspicious_pairs(cmp)
        assert len(suspicious) == 0
