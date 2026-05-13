import pytest

from ptool.checksums import make_stats, HASHERS, IMOHASH_SAMPLE_SIZE


def _write(path, content=b"\x00" * 1024):
    path.write_bytes(content)
    return path


class TestHasherPrefixes:
    """Each hasher must embed its type name as a prefix in the output."""

    def test_imohash_prefix(self, tmp_path):
        f = _write(tmp_path / "f.bin")
        result = make_stats("imohash-64k")(str(f))
        assert not result.has_error()
        assert result.value.startswith("imohash-64k:")

    def test_md5_prefix(self, tmp_path):
        f = _write(tmp_path / "f.bin")
        result = make_stats("md5")(str(f))
        assert not result.has_error()
        assert result.value.startswith("md5:")

    def test_xxhash_prefix(self, tmp_path):
        f = _write(tmp_path / "f.bin")
        result = make_stats("xxhash")(str(f))
        assert not result.has_error()
        assert result.value.startswith("xxhash:")


class TestHasherCorrectness:
    """Full-file hashers (md5, xxhash) must distinguish files that imohash misses."""

    def _make_false_negative_pair(self, tmp_path):
        """Two large files identical in imohash sample windows but different in between."""
        size = 512 * 1024
        base = bytearray(size)
        variant = bytearray(base)
        # Byte at 17 KB — safely between first (0–64 KB) and middle (256–320 KB) windows
        variant[IMOHASH_SAMPLE_SIZE + 1024] = 0xFF
        file_a = tmp_path / "a.bin"
        file_b = tmp_path / "b.bin"
        file_a.write_bytes(bytes(base))
        file_b.write_bytes(bytes(variant))
        return file_a, file_b

    def test_imohash_misses_unsampled_difference(self, tmp_path):
        file_a, file_b = self._make_false_negative_pair(tmp_path)
        hash_a = make_stats("imohash-64k")(str(file_a)).value.split(",")[0]
        hash_b = make_stats("imohash-64k")(str(file_b)).value.split(",")[0]
        assert hash_a == hash_b

    def test_md5_catches_unsampled_difference(self, tmp_path):
        file_a, file_b = self._make_false_negative_pair(tmp_path)
        hash_a = make_stats("md5")(str(file_a)).value.split(",")[0]
        hash_b = make_stats("md5")(str(file_b)).value.split(",")[0]
        assert hash_a != hash_b

    def test_xxhash_catches_unsampled_difference(self, tmp_path):
        file_a, file_b = self._make_false_negative_pair(tmp_path)
        hash_a = make_stats("xxhash")(str(file_a)).value.split(",")[0]
        hash_b = make_stats("xxhash")(str(file_b)).value.split(",")[0]
        assert hash_a != hash_b

    def test_identical_files_agree_across_all_types(self, tmp_path):
        """All hashers should agree that truly identical files have the same hash."""
        content = b"\xAB" * (512 * 1024)
        file_a = tmp_path / "a.bin"
        file_b = tmp_path / "b.bin"
        file_a.write_bytes(content)
        file_b.write_bytes(content)
        for checksum_type in HASHERS:
            hash_a = make_stats(checksum_type)(str(file_a)).value.split(",")[0]
            hash_b = make_stats(checksum_type)(str(file_b)).value.split(",")[0]
            assert hash_a == hash_b, f"{checksum_type} disagreed on identical files"


class TestChecksumTypeMismatchGuard:
    """compare() must reject CSVs generated with different checksum types."""

    def _make_csv(self, tmp_path, checksum_type, filename="snapshot.csv"):
        f = tmp_path / "data.bin"
        f.write_bytes(b"\x00" * 1024)
        record = make_stats(checksum_type)(str(f)).value
        csv_path = tmp_path / filename
        csv_path.write_text(f"checksum,fsize,mtime,fpath\n{record}\n")
        return str(csv_path)

    def test_mismatched_types_raise_error(self, tmp_path):
        import click
        from ptool.analyse import read_csv, compare

        left_dir = tmp_path / "left"
        right_dir = tmp_path / "right"
        left_dir.mkdir()
        right_dir.mkdir()

        left_csv = self._make_csv(left_dir, "imohash-64k")
        right_csv = self._make_csv(right_dir, "md5")

        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)

        with pytest.raises(click.UsageError, match="Checksum type mismatch"):
            compare(left, right)

    def test_matching_types_do_not_raise(self, tmp_path):
        from ptool.analyse import read_csv, compare

        left_dir = tmp_path / "left"
        right_dir = tmp_path / "right"
        left_dir.mkdir()
        right_dir.mkdir()

        left_csv = self._make_csv(left_dir, "xxhash")
        right_csv = self._make_csv(right_dir, "xxhash")

        left, _ = read_csv(left_csv)
        right, _ = read_csv(right_csv)

        # should not raise
        compare(left, right)
