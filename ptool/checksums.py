#!/usr/bin/env python

import os
import re
import stat
import time
import click
from imohash import hashfile
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from itertools import filterfalse


drop_hidden_files_or_dirs = re.compile(r'^[^.]').match


def ignore_re(ignore):
    pats = ignore.split(',')
    res = []
    for i in pats:
        if i.startswith('*'):
            i = '.' + i
        res.append(i)
    pats = "|".join(res)
    return re.compile(pats).search


@contextmanager
def timethis(msg=''):
    st = time.time()
    yield
    elapsed = time.time() - st
    if msg:
        print(f"{msg} Elapsed {elapsed:.2f}s")
    else:
        print(f"Elapsed {elapsed:.2f}s")


def onerror(e):
    print(f"ERROR: {e}")


def hasher(filename):
    return f"imohash:{hashfile(filename, hexdigest=True)}"


def stats(fpath, stat=os.stat):
    try:
        checksum = hasher(fpath)
        st = stat(fpath)
        record = f"{checksum},{st.st_size},{st.st_mtime},{fpath}"
    except:
        print(f"ERROR: skipping {fpath}")
        record = f"-,0,0,{fpath}"
    return record


def get_files(topdir, ignore=None, drop_hidden=drop_hidden_files_or_dirs):
    all_files = []
    if ignore:
        pat = ignore_re(ignore)
    for root, dirs, files in os.walk(topdir, onerror=onerror):
        dirs[:] = list(filter(drop_hidden, dirs))
        files = list(filter(drop_hidden, files))
        if ignore:
            dirs[:] = list(filterfalse(pat, dirs))
            files = list(filterfalse(pat, files))
        files = [os.path.join(root, f) for f in files]
        if files:
            all_files.extend(files)
    return all_files


def main(pool, path, outfile, ignore=None):
    print("Gathering files...")
    with timethis('getting files'):
        if os.path.isdir(path):
            files = get_files(path, ignore=ignore)
        else:
            files = [path]
    nfiles = len(files)
    print(f"nfiles: {nfiles}")
    results = ["checksum,fsize,mtime,fpath"]
    print("Calculating hashes...")
    with timethis('calculating hashes'):
        futures = pool.map(stats, files, chunksize=10)
        for i, item in enumerate(futures):
            if not item.startswith('-'):
                results.append(item)
            print(f"{i:>6d} {item}")
    results = "\n".join(results)
    print(f"Writing results to {outfile.name}")
    outfile.writelines(results)


@click.command()
@click.option("--ignore", default=None, help='ignore dirs or files')
@click.option("--outfile", type=click.File('w'), default='-', help="output filename")
@click.argument("path")
def cli(path, outfile, ignore):
    """path to file or folder.

    Calculates imohash checksum of file(s) at the given path.
    Results are presented as csv.
    """
    path = os.path.expanduser(path)
    pool = ProcessPoolExecutor(max_workers=os.cpu_count())
    main(pool, path=path, outfile=outfile, ignore=ignore)


if __name__ == "__main__":
    cli()
