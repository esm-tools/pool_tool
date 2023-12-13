#!/usr/bin/env python

import os
import re
import stat
import hashlib
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial


CHUNKSIZE = 1024 * 1024 * 10  # 10MB


def get_config():
    site = os.environ.get("POOL_SITE")
    pool = os.environ.get("POOL_NAME")
    conf = os.environ.get("POOL_CONF")
    if conf is None:
        config_file = os.path.join(os.path.dirname(__file__), "config.yaml")
        with open(config_file) as fid:
            config = yaml.safe_load(fid)
        conf = config[site]['pool'][pool]
    else:
        conf = yaml.safe_load(conf)
    return conf


def onerror(e):
    print(f"ERROR: {e}")


def calculate_checksum(filename, name='md5', chunksize=CHUNKSIZE):
    buf = bytearray(chunksize)
    view = memoryview(buf)
    cs = getattr(hashlib, name)()
    with open(filename, "rb") as fileobj:
        readinto = fileobj.readinto
        update = cs.update
        while True:
            size = readinto(buf)
            if size == 0:
                break
            update(view[:size])
    return f"{cs.name}:{cs.hexdigest()}"


def stats(fpath, checksum_name, os=os):
    try:
        checksum = calculate_checksum(fpath, name=checksum_name)
        st = os.stat(fpath)
        record = f"{checksum},{st.st_size},{st.st_mtime},{fpath}"
    except:
        print(f"ERROR: skipping {fpath}")
        record = f"-,0,0,{fpath}"
    return record
        

drop_hidden_files_or_dirs = re.compile(r'^[^.]').match


def get_files(topdir, ignore=None, drop_hidden=drop_hidden_files_or_dirs):
    all_files = []
    for root, dirs, files in os.walk(topdir, onerror=onerror):
        dirs = list(filter(drop_hidden, dirs))
        files = list(filter(drop_hidden, files))
        if ignore:
            dirs[:] = [d for d in dirs if d not in ignore]
            files = [f for f in files if f not in ignore]
        files = [os.path.join(root, f) for f in files]
        if files:
            all_files.extend(files)
    return all_files


def main(pool, topdir, outfile, ignore=None, checksum_name='md5'):
    files = get_files(topdir, ignore=ignore)
    nfiles = len(files)
    print(f"nfiles: {nfiles}")
    _stats = partial(stats, checksum_name=checksum_name)
    results = ["checksum,fsize,mtime,fpath"]
    futures = pool.map(_stats, files, chunksize=10)
    for i, item in enumerate(futures):
        if not item.startswith('-'):
            results.append(item)
        print(f"{i:>6d} {item}")
    results = "\n".join(results)
    print("Writing results to disk")
    with open(outfile, "w") as fid:
        fid.writelines(results)


if __name__ == "__main__":
    if "POOL_SITE" in os.environ:
        conf = get_config()
    else:
        import sys
        site, poolname = sys.argv[1:]
        print(site, poolname)
        os.environ['POOL_SITE'] = site
        os.environ['POOL_NAME'] =  poolname
        conf = get_config()

    topdir = conf.get('path')
    ignore = conf.get('ignore')
    outfile = os.path.expanduser(conf.get('output'))
    checksum = conf.get('checksum')

    pool = ProcessPoolExecutor(max_workers=os.cpu_count())
    main(pool, topdir=topdir, outfile=outfile, ignore=ignore, checksum_name=checksum)
