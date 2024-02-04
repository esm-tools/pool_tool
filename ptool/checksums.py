#!/usr/bin/env python

import os
import re
import stat
import time
import click
from imohash import hashfile
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager


not_hidden_files_or_dirs = re.compile(r"^[^.]").match


def ignore_re(ignore):
    """drops files and folders with the matching pattern.
    `ignore` accepts comma seperated values for matching multiple patterns.
    defaults to None if not provided.
    """
    if not ignore:
        return lambda x: False
    pats = ignore.split(",")
    res = []
    for i in pats:
        if i.startswith("*"):
            i = "." + i
        res.append(i)
    pats = "|".join(res)
    return re.compile(pats).search


@contextmanager
def timethis(msg=""):
    "measures execution time for a given operation"
    st = time.time()
    yield
    elapsed = time.time() - st
    if msg:
        print(f"{msg} Elapsed {elapsed:.2f}s")
    else:
        print(f"Elapsed {elapsed:.2f}s")


def hasher(filename):
    "Calucates imohash for a given file"
    return f"imohash:{hashfile(filename, hexdigest=True)}"


def stats(fpath, stat=os.stat):
    "Generates record with imohash and file stats information"
    try:
        checksum = hasher(fpath)
        st = stat(fpath)
        record = f"{checksum},{st.st_size},{st.st_mtime},{fpath}"
    except Exception as e:
        print(f"{e.__class__.__name__}: {str(e)}")
        #print(f"ERROR: skipping {fpath}")
        record = f"-,0,0,{fpath}"
    return record


def scanner(path, ignore=None, drop_hidden_files=True):
    """Produces iterator object which recursively scans a path.
    Silimar to os.walk but better in performance."""
    path = os.path.expanduser(path)
    dirs = []
    to_ignore = ignore_re(ignore)
    for i in os.scandir(path):
        if i.is_file() and (not to_ignore(i.name)):
            if drop_hidden_files:
                if not_hidden_files_or_dirs(i.name):
                    yield i.path
            else:
                yield i.path
        elif i.is_dir() and (not to_ignore(i.name)):
            if not_hidden_files_or_dirs(i.name):
                dirs.append(i.path)
        elif i.is_symlink():
            try:
                i.stat()
            except FileNotFoundError:
                print(f"skipping.. {i.path} -> {os.readlink(i.path)}")
            except Exception as e:
                print(f"{e.__class__.__name__}: {str(e)}")
            else:
                if os.path.isdir(i.path):
                    dirs.append(i.path)
                elif os.path.isfile(i.path):
                    yield i.path
    for d in dirs:
        yield from scanner(d, ignore, drop_hidden_files)


def get_files(path, ignore=None, drop_hidden_files=True):
    "Wrapper around scanner method to produce a list of files instead of iterator"
    files_iter = scanner(path, ignore=ignore, drop_hidden_files=drop_hidden_files)
    return list(files_iter)


def main(pool, path, outfile, ignore=None, drop_hidden_files=True):
    "Calculates hashs of all the files in parallel"
    print("Gathering files...")
    with timethis("getting files"):
        if os.path.isdir(path):
            files = get_files(path, ignore=ignore, drop_hidden_files=drop_hidden_files)
        else:
            files = [path]
    nfiles = len(files)
    print(f"nfiles: {nfiles}")
    results = ["checksum,fsize,mtime,fpath"]
    print("Calculating hashes...")
    with timethis("calculating hashes"):
        futures = pool.map(stats, files, chunksize=10)
        for i, item in enumerate(futures):
            if not item.startswith("-"):
                results.append(item)
            # print(f"{i:>6d} {item}")
    results = "\n".join(results)
    print(f"Writing results to {outfile.name}")
    outfile.writelines(results)


@click.command()
@click.option(
    "--drop-hidden-files/--no-drop-hidden-files",
    default=True,
    is_flag=True,
    show_default=True,
    help="ignore hidden files",
)
@click.option("--ignore", default=None, show_default=True, help="ignore dirs or files")
@click.option(
    "-o", "--outfile", type=click.File("w"), default="-", help="output filename"
)
@click.argument("path")
def cli(path, outfile, ignore, drop_hidden_files):
    """path to file or folder.

    Calculates imohash checksum of file(s) at the given path.
    Results are presented as csv.
    """
    path = os.path.expanduser(path)
    pool = ProcessPoolExecutor(max_workers=os.cpu_count())
    main(
        pool,
        path=path,
        outfile=outfile,
        ignore=ignore,
        drop_hidden_files=drop_hidden_files,
    )


if __name__ == "__main__":
    cli()
