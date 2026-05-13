import hashlib

from imohash import hashfile

SAMPLE_THRESHOLD = 128 * 1024
SAMPLE_SIZE = 16 * 1024


def _md5(path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


class TestImohashFalseNegatives:

    def test_false_negative_large_files(self, tmp_path):
        """Two large files identical in sampled regions but different in the
        unsampled middle produce the same imohash — demonstrating the known
        false-negative risk for files >= SAMPLE_THRESHOLD."""
        size = 512 * 1024  # 512 KB, well above SAMPLE_THRESHOLD

        base = bytearray(size)

        # Sampled windows for a 512 KB file:
        #   first:  [0 : 16 KB]
        #   middle: [256 KB : 272 KB]
        #   last:   [496 KB : 512 KB]
        # Byte at 17 KB sits safely in the unsampled gap.
        unsampled_offset = SAMPLE_SIZE + 1024  # 17 KB

        variant = bytearray(base)
        variant[unsampled_offset] = 0xFF

        file_a = tmp_path / "file_a.bin"
        file_b = tmp_path / "file_b.bin"
        file_a.write_bytes(bytes(base))
        file_b.write_bytes(bytes(variant))

        # imohash cannot distinguish them — false negative
        assert hashfile(str(file_a), hexdigest=True) == hashfile(str(file_b), hexdigest=True)

        # MD5 correctly identifies the difference
        assert _md5(file_a) != _md5(file_b)

    def test_no_false_negative_small_files(self, tmp_path):
        """Files below SAMPLE_THRESHOLD are hashed in full, so any difference
        is always detected."""
        size = SAMPLE_THRESHOLD - 1

        base = bytearray(size)
        variant = bytearray(base)
        variant[100] = 0xFF

        file_a = tmp_path / "small_a.bin"
        file_b = tmp_path / "small_b.bin"
        file_a.write_bytes(bytes(base))
        file_b.write_bytes(bytes(variant))

        assert hashfile(str(file_a), hexdigest=True) != hashfile(str(file_b), hexdigest=True)

    def test_different_sizes_never_collide(self, tmp_path):
        """imohash encodes file size in the digest, so files of different sizes
        always produce different hashes regardless of content."""
        file_a = tmp_path / "size_a.bin"
        file_b = tmp_path / "size_b.bin"
        file_a.write_bytes(b"\x00" * (512 * 1024))
        file_b.write_bytes(b"\x00" * (512 * 1024 + 1))

        assert hashfile(str(file_a), hexdigest=True) != hashfile(str(file_b), hexdigest=True)
