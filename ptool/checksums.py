#!/usr/bin/env python

import os
import re
import stat
import hashlib
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial


CHUNKSIZE = 1024 * 1024  # 1MB


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


def make_checksum_func(name=None, chunksize=CHUNKSIZE, default='md5'):
    if name and name in hashlib.algorithms_guaranteed:
        func = getattr(hashlib, name)
    else:
        print(f"Using {default} (default)")
        func = getattr(hashlib, default)
    def calculate_checksum(filename, func=func, chunksize=chunksize):
        buf = bytearray(chunksize)
        view = memoryview(buf)
        cs = func()
        with open(filename, "rb") as fileobj:
            readinto = fileobj.readinto
            update = cs.update
            while True:
                size = readinto(buf)
                if size == 0:
                    break
                update(view[:size])
        return f"{cs.name}:{cs.hexdigest()}"
    return calculate_checksum


def stats(fpath, checksum_func, os=os):
    try:
        checksum = checksum_func(fpath)
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
    checksum_func = make_checksum_func(checksum_name)
    stats = partial(stats, checksum_func=checksum_func)
    results = ["checksum,fsize,mtime,fpath"]
    futures = [pool.submit(stats, f) for f in files]
    for item in as_completed(futures):
        results.append(item.result())
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

    pool = ProcessPoolExecutor()
    main(pool, topdir=topdir, outfile=outfile, ignore=ignore, checksum_name=checksum)
