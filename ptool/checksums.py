#!/usr/bin/env python

import fnmatch
import hashlib
import os
import re
import sys
import time
from contextlib import contextmanager
from typing import Callable, List, Optional

import click
from imohash import hashfile
from tqdm.contrib.concurrent import process_map

IMOHASH_SAMPLE_SIZE = 64 * 1024  # upgraded from 16 KB to reduce false-negative risk

not_hidden_files_or_dirs = re.compile(r"^[^.]").match


def getecho():
    file = sys.stdout
    if not sys.stdout.isatty():
        file = sys.stderr

    def _print(*args):
        print(*args, file=file, flush=True)

    return _print


echo = getecho()


def ignore_re(pattern: str = None) -> Callable:
    """matches files or folders based provided pattern.

    If *wildcards* are not present in pattern, then this function looks for
    exact match to the pattern.

    comma separated multiple patterns are allowed.
    """
    pats = [lambda x: False]
    if pattern:
        pats = [fnmatch._compile_pattern(p) for p in split(pattern)]

    def ignore(value: str) -> bool:
        for pat in pats:
            if pat(value):
                return True
        else:
            return False

    return ignore


def split(s: str, sep: str = ",", escape: str = "\\") -> List[Optional[str]]:
    """Split the string with respect to `sep` character

    To preserve `sep` character in the string at certain places, prefix it with
    escape character. The default escape character is "\\" but it can also
    replaced with some other character if it is required. See examples.

    Examples:

    >>> split("core1,core2")
    ["core1", "core2"]
    >>> split("core1\,group,core2")
    ["core1,group", "core2"]
    >>> split("a,b\,c,d")
    ['a', 'b,c', 'd']
    >>> split("a|b\|c|d", sep="|")
    ['a', 'b|c', 'd']
    >>> split("a|b#|c|d", sep="|", escape="#")
    ['a', 'b|c', 'd']
    """
    if not s:
        return []
    empty = ""
    result = []
    tmp = []
    for part in s.split(sep):
        if escape in part:
            tmp.append(part.replace(escape, empty))
        else:
            if tmp:
                tmp.append(part)
                result.append(f"{sep}".join(tmp))
                tmp.clear()
            else:
                result.append(part)
    return result


@contextmanager
def timethis(msg=""):
    "measures execution time for a given operation"
    st = time.time()
    yield
    elapsed = time.time() - st
    if msg:
        echo(f"{msg} Elapsed {elapsed:.2f}s")
    else:
        echo(f"Elapsed {elapsed:.2f}s")


class Results:
    "Wraps the result (either successful result or an exception)"

    def __init__(self, value=None, exc=None):
        self.value = value
        self.exc = exc

    def has_error(self):
        return self.exc is not None

    def result(self):
        if self.exc:
            return self.exc
        return self.value


def _imohash(filename):
    return f"imohash-64k:{hashfile(filename, sample_size=IMOHASH_SAMPLE_SIZE, hexdigest=True)}"


def _md5(filename):
    h = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return f"md5:{h.hexdigest()}"


def _xxhash(filename):
    import xxhash
    h = xxhash.xxh3_64()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return f"xxhash:{h.hexdigest()}"


HASHERS = {
    "imohash-64k": _imohash,
    "md5": _md5,
    "xxhash": _xxhash,
}


def make_stats(checksum_type):
    "Returns a stats function bound to the given checksum type"
    hasher = HASHERS[checksum_type]

    def stats(fpath, stat=os.stat):
        try:
            checksum = hasher(fpath)
            st = stat(fpath)
            record = f"{checksum},{st.st_size},{st.st_mtime},{fpath}"
            return Results(value=record)
        except Exception as e:
            return Results(exc=f"{str(e)}")

    return stats


# default stats function for backwards-compatible use
stats = make_stats("imohash-64k")


def scanner(path, ignore=None, ignore_dirs=None, drop_hidden_files=True):
    """Produces iterator object which recursively scans a path.
    Silimar to os.walk but better in performance."""
    path = os.path.expanduser(path)
    dirs = []
    to_ignore = ignore_re(ignore)
    to_ignore_dirs = ignore_re(ignore_dirs)
    for i in os.scandir(path):
        if i.is_file() and (not to_ignore(i.name)):
            if drop_hidden_files:
                if not_hidden_files_or_dirs(i.name):
                    yield i.path
            else:
                yield i.path
        elif i.is_dir() and (not to_ignore_dirs(i.name)):
            if not_hidden_files_or_dirs(i.name):
                dirs.append(i.path)
        elif i.is_symlink():
            try:
                i.stat()
            except FileNotFoundError:
                echo(f"skipping.. {i.path} -> {os.readlink(i.path)}")
            except Exception as e:
                echo(f"{e.__class__.__name__}: {str(e)}")
            else:
                if os.path.isdir(i.path):
                    dirs.append(i.path)
                elif os.path.isfile(i.path):
                    yield i.path
    for d in dirs:
        yield from scanner(d, ignore, ignore_dirs, drop_hidden_files)


def get_files(path, ignore=None, ignore_dirs=None, drop_hidden_files=True):
    "Wrapper around scanner method to produce a list of files instead of iterator"
    files_iter = scanner(
        path,
        ignore=ignore,
        ignore_dirs=ignore_dirs,
        drop_hidden_files=drop_hidden_files,
    )
    return list(files_iter)


def main(path, outfile, ignore=None, ignore_dirs=None, drop_hidden_files=True, checksum_type="imohash-64k"):
    "Calculates hashes of all the files in parallel"
    echo("Gathering files...")
    with timethis("getting files"):
        if os.path.isdir(path):
            files = get_files(
                path,
                ignore=ignore,
                ignore_dirs=ignore_dirs,
                drop_hidden_files=drop_hidden_files,
            )
        else:
            files = [path]
    nfiles = len(files)
    echo(f"nfiles: {nfiles}")
    results = ["checksum,fsize,mtime,fpath"]
    echo(f"Calculating hashes ({checksum_type})...")
    errors = []
    stats_fn = make_stats(checksum_type)
    with timethis("calculating hashes"):
        futures = process_map(
            stats_fn, files, chunksize=10, max_workers=os.cpu_count(), unit="files"
        )
        for item in futures:
            if item.has_error():
                errors.append(item)
            else:
                results.append(item.result())
    results = "\n".join(results)
    if errors:
        nerrors = len(errors)
        errorstr = "\n".join([e.result() for e in errors])
        echo(errorstr)
        echo(f"Found {nerrors} Errors out of {nfiles} Files")
    echo(f"Writing results to {outfile.name}")
    outfile.writelines(results)


@click.command()
@click.option(
    "--drop-hidden-files/--no-drop-hidden-files",
    default=True,
    is_flag=True,
    show_default=True,
    help="ignore hidden files",
)
@click.option("--ignore", default=None, show_default=True, help="ignore files")
@click.option(
    "--ignore-dirs", default=None, show_default=True, help="ignore directories"
)
@click.option(
    "-o", "--outfile", type=click.File("w"), default="-", help="output filename"
)
@click.option(
    "--checksum-type",
    type=click.Choice(list(HASHERS)),
    default="imohash-64k",
    show_default=True,
    help="checksum algorithm to use. imohash-64k is fast (samples 3×64KB) but "
         "may miss changes in unsampled regions of large files. xxhash and md5 "
         "read the full file and are collision-free but slower on large files.",
)
@click.argument("path")
def cli(path, outfile, ignore, ignore_dirs, drop_hidden_files, checksum_type):
    """path to file or folder.

    Calculates checksum of file(s) at the given path.
    Results are presented as csv.
    """
    path = os.path.expanduser(path)
    main(
        path=path,
        outfile=outfile,
        ignore=ignore,
        ignore_dirs=ignore_dirs,
        drop_hidden_files=drop_hidden_files,
        checksum_type=checksum_type,
    )


if __name__ == "__main__":
    cli()
