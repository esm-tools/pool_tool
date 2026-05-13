# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Configurable checksum types: `imohash-64k` (default), `xxhash`, `md5`
- `--checksum-type` option on `ptool checksums` command
- Guard in `compare` and `summary` against mixing CSVs with different checksum types
- Test suite covering checksum type correctness and false-negative behaviour
- `pyproject.toml` replacing legacy `setup.py`
- `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `CHANGELOG.md`
- GitHub Actions CI workflow
- GitHub issue and PR templates

### Fixed
- Invalid `str[pyarrow]` dtype string in `read_csv` (now `string[pyarrow]`), which caused a `TypeError` on pandas 2.x
- Incorrect `license = "MIT"` metadata — corrected to GPL-3.0 to match the `LICENSE` file
- Invalid PyPI classifier `Development Status :: 0.2.2` — corrected to `Development Status :: 4 - Beta`
- Wrong `url` in package metadata (pointed to GitLab) — updated to GitHub

### Changed
- imohash sample size upgraded from 16 KB to 64 KB to reduce false-negative risk

## [0.2.2] - 2024-01-01

### Added
- `sync_options`: support for additional sync configuration options

## [0.2.1] - 2024-01-01

### Fixed
- Truncated prefix handling in checksum paths

## [0.2.0] - 2024-01-01

### Added
- Initial public release with `checksums`, `compare`, `summary`, and `prepare-rsync` commands
