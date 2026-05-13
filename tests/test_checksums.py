import os

import pytest

from ptool.checksums import Results, get_files, hasher, ignore_re, scanner, split, stats


class TestSplit:
    def test_simple(self):
        assert split("a,b,c") == ["a", "b", "c"]

    def test_single(self):
        assert split("a") == ["a"]

    def test_empty(self):
        assert split("") == []

    def test_none(self):
        assert split(None) == []

    def test_escaped_separator(self):
        assert split(r"a\,b,c") == ["a,b", "c"]

    def test_escaped_separator_middle(self):
        assert split(r"a,b\,c,d") == ["a", "b,c", "d"]

    def test_custom_separator(self):
        assert split("a|b|c", sep="|") == ["a", "b", "c"]

    def test_custom_escape(self):
        assert split("a|b#|c|d", sep="|", escape="#") == ["a", "b|c", "d"]


class TestIgnoreRe:
    def test_no_pattern_matches_nothing(self):
        fn = ignore_re(None)
        assert fn("anything") is False

    def test_exact_match(self):
        fn = ignore_re("foo.nc")
        assert fn("foo.nc") is True
        assert fn("bar.nc") is False

    def test_wildcard(self):
        fn = ignore_re("*.nc")
        assert fn("data.nc") is True
        assert fn("data.csv") is False

    def test_multiple_patterns(self):
        fn = ignore_re("*.nc,*.txt")
        assert fn("data.nc") is True
        assert fn("readme.txt") is True
        assert fn("script.py") is False


class TestResults:
    def test_value(self):
        r = Results(value="ok")
        assert not r.has_error()
        assert r.result() == "ok"

    def test_exception(self):
        r = Results(exc="something went wrong")
        assert r.has_error()
        assert r.result() == "something went wrong"


class TestHasher:
    def test_returns_imohash_prefix(self, tmp_path):
        f = tmp_path / "file.bin"
        f.write_bytes(b"\x00" * 1024)
        result = hasher(str(f))
        assert result.startswith("imohash:")

    def test_same_content_same_hash(self, tmp_path):
        content = b"\xAB" * 1024
        a = tmp_path / "a.bin"
        b = tmp_path / "b.bin"
        a.write_bytes(content)
        b.write_bytes(content)
        assert hasher(str(a)) == hasher(str(b))

    def test_different_content_different_hash(self, tmp_path):
        a = tmp_path / "a.bin"
        b = tmp_path / "b.bin"
        a.write_bytes(b"\x00" * 1024)
        b.write_bytes(b"\xFF" * 1024)
        assert hasher(str(a)) != hasher(str(b))


class TestStats:
    def test_valid_file_produces_csv_record(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"\x00" * 512)
        result = stats(str(f))
        assert not result.has_error()
        parts = result.value.split(",")
        assert len(parts) == 4
        assert parts[0].startswith("imohash:")
        assert parts[1] == "512"  # fsize
        assert str(f) in parts[3]  # fpath

    def test_missing_file_produces_error(self, tmp_path):
        result = stats(str(tmp_path / "nonexistent.bin"))
        assert result.has_error()


class TestScanner:
    def test_finds_files(self, tmp_path):
        (tmp_path / "a.nc").write_bytes(b"x")
        (tmp_path / "b.nc").write_bytes(b"x")
        found = list(scanner(str(tmp_path)))
        assert len(found) == 2

    def test_recurses_into_subdirs(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "root.nc").write_bytes(b"x")
        (sub / "nested.nc").write_bytes(b"x")
        found = list(scanner(str(tmp_path)))
        assert len(found) == 2

    def test_drops_hidden_files_by_default(self, tmp_path):
        (tmp_path / "visible.nc").write_bytes(b"x")
        (tmp_path / ".hidden").write_bytes(b"x")
        found = list(scanner(str(tmp_path)))
        assert len(found) == 1
        assert all(".hidden" not in f for f in found)

    def test_includes_hidden_files_when_disabled(self, tmp_path):
        (tmp_path / "visible.nc").write_bytes(b"x")
        (tmp_path / ".hidden").write_bytes(b"x")
        found = list(scanner(str(tmp_path), drop_hidden_files=False))
        assert len(found) == 2

    def test_ignore_pattern(self, tmp_path):
        (tmp_path / "keep.nc").write_bytes(b"x")
        (tmp_path / "skip.tmp").write_bytes(b"x")
        found = list(scanner(str(tmp_path), ignore="*.tmp"))
        assert len(found) == 1
        assert found[0].endswith("keep.nc")

    def test_ignore_dirs(self, tmp_path):
        keep = tmp_path / "keep"
        skip = tmp_path / "skip"
        keep.mkdir()
        skip.mkdir()
        (keep / "a.nc").write_bytes(b"x")
        (skip / "b.nc").write_bytes(b"x")
        found = list(scanner(str(tmp_path), ignore_dirs="skip"))
        assert len(found) == 1
        assert "keep" in found[0]


class TestGetFiles:
    def test_returns_list(self, tmp_path):
        (tmp_path / "a.nc").write_bytes(b"x")
        result = get_files(str(tmp_path))
        assert isinstance(result, list)
        assert len(result) == 1
