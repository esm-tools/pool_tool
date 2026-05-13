import hashlib

from imohash import hashfile

SAMPLE_THRESHOLD = 128 * 1024
SAMPLE_SIZE = 16 * 1024


def _md5(path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


class TestImohashFalseNegatives:

    def test_false_negative_large_files(self, tmp_path):
        """Two large files identical in sampled regions but different in the
        unsampled middle produce the same imohash — the known false-negative risk."""
        size = 512 * 1024

        base = bytearray(size)
        # First window:  [0 : SAMPLE_SIZE]              → [0 : 16 KB]
        # Middle window: [size//2 : size//2+SAMPLE_SIZE] → [256 KB : 272 KB]
        # End window:    [size-SAMPLE_SIZE : size]       → [496 KB : 512 KB]
        # Unsampled gap: (16 KB, 256 KB) — safe to modify without touching any window
        unsampled_offset = SAMPLE_SIZE + 1024  # 17 KB

        variant = bytearray(base)
        variant[unsampled_offset] = 0xFF

        file_a = tmp_path / "file_a.bin"
        file_b = tmp_path / "file_b.bin"
        file_a.write_bytes(bytes(base))
        file_b.write_bytes(bytes(variant))

        assert hashfile(str(file_a), hexdigest=True) == hashfile(str(file_b), hexdigest=True), (
            "imohash should produce the same digest for files differing only in the unsampled region"
        )
        assert _md5(file_a) != _md5(file_b), "MD5 should correctly detect the difference"

    def test_no_false_negative_small_files(self, tmp_path):
        """Files below SAMPLE_THRESHOLD are read fully — no false negatives."""
        size = SAMPLE_THRESHOLD - 1

        base = bytearray(size)
        variant = bytearray(base)
        variant[100] = 0xFF

        file_a = tmp_path / "small_a.bin"
        file_b = tmp_path / "small_b.bin"
        file_a.write_bytes(bytes(base))
        file_b.write_bytes(bytes(variant))

        assert hashfile(str(file_a), hexdigest=True) != hashfile(str(file_b), hexdigest=True), (
            "Small files are fully hashed — any difference must be detected"
        )

    def test_different_sizes_never_collide(self, tmp_path):
        """imohash encodes file size in the digest, so files of different sizes
        always produce different hashes regardless of content."""
        file_a = tmp_path / "size_a.bin"
        file_b = tmp_path / "size_b.bin"
        file_a.write_bytes(b"\x00" * (512 * 1024))
        file_b.write_bytes(b"\x00" * (512 * 1024 + 1))

        assert hashfile(str(file_a), hexdigest=True) != hashfile(str(file_b), hexdigest=True)
