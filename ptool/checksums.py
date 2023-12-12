#!/usr/bin/env python

import os
import re
import stat
import hashlib
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed


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


def stats(fname):
    try:
        checksum = md5(fname)
        st = os.stat(fname)
        record = f"{checksum},{st.st_size},{st.st_mtime},{fname}"
    except:
        print(f"ERROR: skipping {fname}")
        record = f"-,0,0,{fname},"
    return record
        

drop_hidden_files_or_dirs = re.compile(r'^[^.]').match


def get_files(topdir, ignore=ignore, drop_hidden=drop_hidden_files_or_dirs):
    all_files = []
    for root, dirs, files in os.walk(topdir, onerror=onerror):
        dirs = list(filter(drop_hidden, dirs))
        dirs[:] = [d for d in dirs if d not in ignore]
        files = list(filter(drop_hidden, files))
        files = [f for f in files if f not in ignore]
        files = [os.path.join(root, f) for f in files]
        if files:
            all_files.extend(files)
    return all_files


def main(pool, topdir, outfile, ignore=None):
    files = get_files(topdir, ignore=ignore)
    nfiles = len(files)
    print(f"nfiles: {nfiles}")
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

    topdir = conf.get('topdir')
    ignore = conf.get('ignore')
    outfile = os.path.expanduser(conf.get('output'))

    pool = ProcessPoolExecutor()
    main(pool)
