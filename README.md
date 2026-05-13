# ptool

[![CI](https://github.com/esm-tools/pool_tool/actions/workflows/ci.yml/badge.svg)](https://github.com/esm-tools/pool_tool/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org)

A tool to manage data pools across different HPC sites. With ptool you can take
snapshots of a pool at regular intervals and compare them to monitor changes over
time. Snapshots can be taken on the same machine or on different machines, and
ptool provides tools to synchronise the state between pools on different machines.

## Contents

- [How it works](#how-it-works)
  - [File association](#file-association)
  - [Folder mapping](#folder-mapping)
- [Installation](#installation)
- [Usage](#usage)
  - [checksums](#checksums-snapshot-of-a-pool)
  - [summary](#summary)
  - [compare](#compare)
  - [prepare-rsync](#prepare-rsync)
- [Checksum algorithms](#checksum-algorithms)
- [Contributing](#contributing)

## How it works

ptool compares pools by taking a **snapshot** (a CSV of checksums + metadata) of each site and
analysing the two CSVs locally. No direct connection between sites is required at analysis time.

### File association

Every file pair is classified using two signals: whether the checksum matches and whether the
filename matches.

| Checksum | Filename | Classification |
|----------|----------|----------------|
| same | same | `identical` — exact copy |
| different | same | `modified` — content changed; mtime indicates which copy is newer |
| same | different | `renamed` — content preserved, name changed |
| different | different | `unique` — file exists only on the left site |

`compare` and `summary` use these labels to report what needs to be transferred and what is
already in sync.

### Folder mapping

Pools on different sites often use different directory layouts. ptool automatically maps
corresponding folders by finding which folder pair has the highest number of matching files
(**max-association wins**). Each folder on the left is paired with at most one folder on the right.

The `--threshold` option (default `0.1`) controls false-positive filtering: if fewer than 10 % of
files in a left folder are associated with a right folder, the association is treated as noise and
those files are reclassified as `unique`. Raise the threshold to be stricter; lower it to allow
sparse associations.

## Installation

Clone the repository on the machine where ptool needs to be installed:

```shell
git clone https://github.com/esm-tools/pool_tool.git
cd pool_tool
```

Create a conda environment and install the package:

```shell
conda env create -f environment.yaml
conda activate ptool
pip install -e .
```

For development (includes test dependencies):

```shell
pip install -e ".[dev]"
pytest tests/ -v
```

## Usage

ptool provides four commands to manage pools:

| Command | Purpose |
|---------|---------|
| `checksums` | Create a snapshot of a pool as a CSV |
| `summary` | High-level overview of two snapshots |
| `compare` | Per-file comparison of two snapshots |
| `prepare-rsync` | Generate a script to transfer files between sites |

### checksums (snapshot of a pool)

```shell
$ ptool checksums --help
Usage: ptool checksums [OPTIONS] PATH

  Calculates checksum of file(s) at the given path.
  Results are presented as csv.

  `--ignore` and `--ignore-dirs` support *wildcards* in filtering down the
  matches.  If no *wildcards* are provided, then it performs a literal match.
  For multiple patterns, use comma separation.

Options:
  --drop-hidden-files / --no-drop-hidden-files
                                  ignore hidden files  [default: drop-hidden-files]
  --ignore TEXT                   ignore files
  --ignore-dirs TEXT              ignore directories
  -o, --outfile FILENAME          output filename
  --help                          Show this message and exit.
```

Taking a snapshot of the `fesom2` pool on Levante:

```shell
$ ptool checksums --ignore-dirs dist_* -o levante_fesom2.csv /pool/data/AWICM/FESOM2
Gathering files...
skipping.. /pool/data/AWICM/FESOM2/FORCING/ERA5 -> /mnt/lustre01/work/ba1138/a270099/era5/forcing/inverted
getting files Elapsed 0.22s
nfiles: 3060
Calculating hashes...
100%|██████████| 3060/3060 [00:01<00:00, 1610.56files/s]
calculating hashes Elapsed 3.21s
Writing results to levante_fesom2.csv
```

The output CSV has four columns: `checksum`, `fsize`, `mtime`, `fpath`.

> **Note:** The filename you choose for the CSV is used as the site label in
> analysis output, so pick a meaningful name (e.g. `levante_fesom2.csv`).

#### Remote checksums

You can invoke `checksums` on a remote machine via SSH and pipe the output
to a local file. This is useful when you want to collect snapshots on the
machine where analysis will be run:

```shell
$ ssh a270243@levante.dkrz.de \
    "~/miniforge3/envs/ptool/bin/ptool checksums /pool/data/AWICM/FESOM2 --ignore-dirs dist_*" \
    > levante_fesom2.csv
```

When no `-o/--outfile` is given, results are written to stdout and can be
redirected to a local file.

### summary

Get a high-level overview of two snapshots:

```shell
$ ptool summary --compact levante_fesom2.csv albedo_fesom2.csv

Table 1: Summary with respect to LEVANTE_FESOM2

                 levante_fesom2            albedo_fesom2
---------------  ------------------------  -------------------
pool             FESOM2                    FESOM2
checksum file    levante_fesom2.csv        albedo_fesom2.csv
prefix           /pool/data/AWICM/FESOM2/  /albedo/pool/FESOM2
files            3031 (29.8 TB)            1577 (1.1 TB)
duplicate files  56 (615.2 MB)             167 (5.6 GB)
identical files  547 (44.8 GB)             547 (44.8 GB)
unique files     2458 (29.8 TB)            nan
modified files   nan                       9 (17.5 MB)
----------------------------------------------------------------------

Table 2: Common directory mapping

    rparent_levante_fesom2    rparent_albedo_fesom2
--  ------------------------  ----------------------------
 0  FORCING/CORE2             /forcing/CORE2
 1  INITIAL/phc3.0            /hydrography_dsidoren/phc3.0
 2  MESHES/CORE2              /core2_meanz_broken
 3  MESHES/CORE2/figures      /core2_meanz_broken/figures
 4  MESHES_FESOM2.1/core2     /core2
 5  MESHES_FESOM2.1/hr        /HR
 6  MESHES_FESOM2.1/mr        /mr
----------------------------------------------------------------------

Table 3: LEVANTE_FESOM2 perspective, per directory associations

                       modified      identical  unique      total
---------------------  ----------  -----------  --------  -------
MESHES_FESOM2.1/hr     1                    14  1              16
MESHES_FESOM2.1/mr     1                    14  1              16
MESHES_FESOM2.1/core2  7                    18  11             36
FORCING/CORE2          -                   479  -             479
INITIAL/phc3.0         -                     5  -               5
MESHES/CORE2/figures   -                     8  -               8
MESHES/CORE2           -                     9  -               9
----------------------------------------------------------------------
```

Run `ptool summary --help` to see all available options.

### compare

Get per-file comparison results:

```shell
$ ptool compare -o lev_alb_fesom2_cmp.csv levante_fesom2.csv albedo_fesom2.csv
Writing results as csv to file lev_alb_fesom2_cmp.csv
                                          rpath_left                         rpath_right
flag
identical          FORCING/CORE2/ncar_precip.1948.nc  /forcing/CORE2/ncar_precip.1948.nc
identical          FORCING/CORE2/ncar_precip.1952.nc  /forcing/CORE2/ncar_precip.1952.nc
...
unique     FORCING/era5/forcing/inverted/t2m.1972.nc                                 NaN

[3014 rows x 2 columns]
```

Each row is classified as one of: `identical`, `renamed`, `modified_latest_left`,
`modified_latest_right`, or `unique`.

You can search the output with `grep`:

```shell
$ grep MESHES_FESOM2.1/hr lev_alb_fesom2_cmp.csv
identical,MESHES_FESOM2.1/hr/edgenum.out,/HR/edgenum.out
...
modified_latest_right,MESHES_FESOM2.1/hr/README.md,/HR/README.md
unique,MESHES_FESOM2.1/hr/README,
```

> **Important:** Both CSVs must have been generated with the same checksum
> algorithm. ptool will raise an error if you try to compare snapshots taken
> with different algorithms.

### prepare-rsync

Generates a shell script containing rsync commands to transfer files between
sites. Always review the script before executing it.

```shell
$ ptool prepare-rsync --help
Usage: ptool prepare-rsync [OPTIONS] LEFT RIGHT

  Prepares rsync commands for the transfer.

  Depending on where data needs to be pushed or pulled, provide either
  `--lefthost` or `--righthost` to prefix that path.

  Note: when Albedo is involved, run this command on Albedo and provide the
  other host information, as Albedo cannot be reached from external machines.

Options:
  --ignore TEXT                   ignores directory and files
  --flags [unique|modified|both]  association type to include
  -t, --threshold FLOAT           minimum value to satisfy valid association
                                  [default: 0.1]
  -l, --lefthost TEXT             username@host prefix for the left path
  -r, --righthost TEXT            username@host prefix for the right path
  --help                          Show this message and exit.
```

Transferring files from Levante to Albedo (run this on Albedo):

```shell
$ ptool prepare-rsync --lefthost a270243@levante.dkrz.de levante_fesom2.csv albedo_fesom2.csv
Created sync_cmd.sh
```

Verify the contents of `sync_cmd.sh` before executing:

```shell
$ grep MESHES_FESOM2.1/hr sync_cmd.sh
# MESHES_FESOM2.1/hr
rsync -av --files-from=flist/95087e3b a270243@levante.dkrz.de:/pool/data/AWICM/FESOM2/MESHES_FESOM2.1/hr/ /albedo/pool/FESOM2/HR/
```

#### Examples that work (run on Albedo)

```shell
# Sync data: Levante → Albedo
ptool prepare-rsync --lefthost user@levante.dkrz.de levante_fesom2.csv albedo_fesom2.csv

# Sync data: Albedo → Levante
ptool prepare-rsync --righthost user@levante.dkrz.de albedo_fesom2.csv levante_fesom2.csv
```

#### Examples that fail (run on Levante)

```shell
# This fails — Albedo is not reachable from external machines
ptool prepare-rsync --righthost user@albedo0.dmawi.de levante_fesom2.csv albedo_fesom2.csv
```

## Checksum algorithms

ptool uses [imohash](https://github.com/kalafut/py-imohash) by default. imohash
is a fast sampling hasher: for files larger than 128 KB it reads only three
16 KB windows (start, middle, end) rather than the full file. This makes
snapshot generation very fast even for large pools of climate data.

**Trade-off:** Two files that are identical in the sampled regions but differ
elsewhere will be incorrectly classified as identical (a false negative). For
most sync workflows this risk is acceptably low, but it is worth being aware of.

| Algorithm | Reads | False negatives possible? | Best for |
|-----------|-------|--------------------------|----------|
| `imohash` | 3 × 16 KB sample | Yes, for large files | Fast snapshots of large pools |

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for
setup instructions and the contribution workflow.

## License

This project is licensed under the GNU General Public License v3.0 — see the
[LICENSE](LICENSE) file for details.
