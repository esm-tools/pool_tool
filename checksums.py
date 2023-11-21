#!/usr/bin/env python

import os
import stat
import hashlib
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed


def get_config():
    site = os.environ.get("POOL_SITE")
    pool = os.environ.get("POOL_NAME")
    with open("config.yaml") as fid:
        config = yaml.load(fid, yaml.loader.SafeLoader)
    for sites in config:
        if sites['site'] == site:
            break
    for pools in site:
        if pools['pool'] == pool:
            break
    conf = pools['pool']
    return conf


conf = get_config()

topdir = conf.get('path')
ignore = conf.get('ignore')
outfile = conf.get('output')

prefix_size = len(topdir)
results = ["checksum,fsize,mtime,fname"]

CHUNKSIZE = 1024 * 1024  # 1MB

def onerror(e):
    print(f"ERROR: {e}")


def get_checksum_func(name=None, default='md5'):
    if name and name in hashlib.algorithms_guaranteed:
        return getattr(hashlib, name)
    if name:
        allowed = ','.join(sorted(hashlib.algorithms_guaranteed))
        print(f"'{name}' not in : {allowed}")
        print(f"Using {default} instead.")
    else:
        print(f"Using {default}")
    return getattr(hashlib, default)


def md5(fname, chunksize=CHUNKSIZE):
    hash_md5 = hashlib.md5()
    buf = bytearray(chunksize)
    view = memoryview(buf)
    with open(fname, "rb") as fileobj:
        while True:
            size = fileobj.readinto(buf)
            if size == 0:
                break
            hash_md5.update(view[:size])
    return hash_md5.hexdigest()


def stats(fname, prefix=topdir, prefix_size=prefix_size, sep=os.path.sep, dirname=os.path.dirname):
    try:
        checksum = md5(fname)
        st = os.stat(fname)
        record = f"{checksum},{st.st_size},{st.st_mtime},{fname}"
    except:
        print(f"ERROR: skipping {fname}")
        record = f"-,0,0,{fname},"
    return record
        

def get_files(topdir, ignore=ignore):
    all_files = []
    for root, dirs, files in os.walk(topdir, onerror=onerror):
        dirs[:] = [d for d in dirs if d not in ignore]
        files = [os.path.join(root, f) for f in files if not f.startswith('.')]
        if files:
            all_files.extend(files)
    return all_files


def main(pool, results=results):
    files = get_files(topdir)
    nfiles = len(files)
    print(f"nfiles: {nfiles}")
    futures = [pool.submit(stats, f) for f in files]
    for item in as_completed(futures):
        results.append(item.result())
    results = "\n".join(results)
    print("Writing results to disk")
    with open(outfile, "w") as fid:
        fid.writelines(results)


if __name__ == "__main__":
    pool = ProcessPoolExecutor()
    main(pool)
