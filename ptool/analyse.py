import os
import pandas as pd
import humanize


def read_csv(filename, ignore=None):
    df = pd.read_csv(filename, engine='pyarrow')
    df = df.rename(columns={'fname': 'fpath'})
    df = df[df.checksum != '-']
    df['fname'] = df.fpath.apply(os.path.basename)
    df['prefix'] = os.path.commonprefix(list(df.fpath))
    df['rpath'] = df.fpath.str.removeprefix(df.prefix[0])
    df['rparent'] = df.rpath.apply(os.path.dirname)
    for name, dtype in df.dtypes.items():
        if dtype == 'object':
            df[name] = df[name].astype("str[pyarrow]")
    if ignore:
        df = df[~df.rparent.str.contains(ignore)]
        df = df[~df.fname.str.contains(ignore)]
    dups = df[df.sort_values(by=['checksum', 'mtime']).duplicated(subset='checksum').values]
    df = df.sort_values(by=['checksum', 'mtime']).drop_duplicates(subset='checksum')
    df['mtime'] = pd.to_datetime(df['mtime'], unit='s')
    dups['mtime'] = pd.to_datetime(dups['mtime'], unit='s')
    _, pool, site = filename.split('_')
    site, _ = os.path.splitext(site)
    df.filename = filename
    df.pool = pool
    df.site = site
    dups.filename = filename
    dups.pool = pool
    dups.site = site
    return df, dups


def _group_with_max_counts(df, key='rparent_right'):
    a = [(len(group), group) for gname, group in df.groupby(key)]
    count, group = sorted(a, key=lambda x: x[0]).pop()
    return group


def merge(dl, da, on='checksum', how='inner'):
    m = pd.merge(dl, da, on=on, how=how, suffixes=('_left', '_right'))
    mm = m.groupby('rparent_left').apply(_group_with_max_counts).reset_index(drop=True)
    mm = mm.groupby('rparent_right').apply(lambda x: _group_with_max_counts(x, key='rparent_left')).reset_index(drop=True)
    return mm


def directorymap(m):
    return (m[['rparent_left', 'rparent_right']]).drop_duplicates()


def compare(left, right, relabel=False):
    by_hash = merge(left, right)
    by_name = merge(left, right, on='fname')
    by_hash['flag'] = ''
    by_name['flag'] = ''
    common_hashes = list(by_hash.checksum)
    common_names = list(by_name.checksum_left)
    common_cs = list(set(common_hashes + common_names))
    renamed_mask = by_hash.fname_left != by_hash.fname_right
    renamed_df = by_hash[renamed_mask]
    results = {}
    ## identify renamed files (checksum matches but not filename)
    if not renamed_df.empty:
        renamed_df = renamed_df.copy()
        renamed_df['flag'] = 'renamed'
        renamed_df.set_index('flag', inplace=True)
        renamed_df['checksum_right'] = renamed_df['checksum']
        renamed_df.rename(columns={'checksum': 'checksum_left'}, inplace=True)
        results['renamed'] = renamed_df
        by_hash = by_hash[~renamed_mask]
    ## identify identical files (both checksum and filename match)
    by_hash = by_hash.copy()
    by_hash['flag'] = 'identical'
    by_hash.set_index('flag', inplace=True)
    by_hash['checksum_right'] = by_hash['checksum']
    by_hash.rename(columns={'checksum': 'checksum_left'}, inplace=True)
    results['identical'] = by_hash
    ## identify modified files (filename matches but not checksum)
    ## latest mtime in the file-pair is the most recent one
    ## This means, indicating which of the pairs is latest is useful
    modified = by_name[by_name.checksum_left != by_name.checksum_right]
    left_latest = modified[modified.mtime_left > modified.mtime_right]
    if not left_latest.empty:
        left_latest = left_latest.copy()
        left_latest['flag'] = 'modified_latest_left'
        left_latest.set_index('flag', inplace=True)
        left_latest['fname_right'] = left_latest['fname']
        left_latest.rename(columns={'fname': 'fname_left'}, inplace=True)
        results['modified_latest_left'] = left_latest
    right_latest = modified[modified.mtime_left < modified.mtime_right]
    if not right_latest.empty:
        right_latest = right_latest.copy()
        right_latest['flag'] = 'modified_latest_right'
        right_latest.set_index('flag', inplace=True)
        right_latest['fname_right'] = right_latest['fname']
        right_latest.rename(columns={'fname': 'fname_left'}, inplace=True)
        results['modified_latest_right'] = right_latest
    ## unique files (files found only on left site (i.e., first argument))
    left_only = left[~left.checksum.isin(common_cs)]
    if not left_only.empty:
        left_only = left_only.copy()
        left_only['flag'] = 'unique'
        left_only.set_index('flag', inplace=True)
        cols = [c + '_left' for c in left_only.columns]
        left_only.columns = cols
        results['unique'] = left_only
    order = {'identical': 1, 'renamed': 2, 'modified_latest_left': 3, 'modified_latest_right': 4, 'unique': 5}
    results = pd.concat([results[key] for key in sorted(results, key=order.get)])
    if relabel:
        newcols = [c.replace('left', left.site).replace('right', right.site) for c in results.columns]
        results.columns = newcols
    return results


def compare_compact(left, right, columns='rpath', relabel=False):
    df = compare(left, right)
    if isinstance(columns, str):
        columns = columns.split(',')
    cols = []
    for col in columns:
        cols.append(col + "_left")
    for col in columns:
        cols.append(col + "_right")
    df = df[cols]
    if relabel:
        df = df[cols]
        newcols = [c.replace('left', left.site).replace('right', right.site) for c in df.columns]
        df.columns = newcols
    return df


def summary(filename1, filename2, ignore=None):
    left, left_dups = read_csv(filename1, ignore=ignore)
    right, right_dups = read_csv(filename2, ignore=ignore)
    _, left_pool, left_site = filename1.split('_')
    left_site, _ = os.path.splitext(left_site)
    _, right_pool, right_site = filename2.split('_')
    right_site, _ = os.path.splitext(right_site)
    hsize = lambda x: humanize.naturalsize(x)
    dset = {}
    dset[left_site] = {
        'pool': left_pool,
        'checksum file': filename1,
        'prefix': left.prefix.iloc[0],
        'files': f"{left.shape[0]} ({hsize(left.fsize.sum())})",
        'duplicate files': f"{left_dups.shape[0]} ({hsize(left_dups.fsize.sum())})"
    }
    dset[right_site] = {
        'pool': right_pool,
        'checksum file': filename2,
        'prefix': right.prefix.iloc[0],
        'files': f"{right.shape[0]} ({hsize(right.fsize.sum())})",
        'duplicate files': f"{right_dups.shape[0]} ({hsize(right_dups.fsize.sum())})"
    }
    cmp = compare(left, right)
    identical = cmp.loc['identical']
    dset[left_site]['identical files'] = f"{identical.shape[0]} ({hsize(identical.fsize_left.sum())})"
    dset[right_site]['identical files'] = f"{identical.shape[0]} ({hsize(identical.fsize_right.sum())})"
    renamed = cmp.loc['renamed']
    dset[right_site]['renamed files'] = f"{renamed.shape[0]} ({hsize(renamed.fsize_left.sum())})"
    if 'modified_latest_left' in cmp.index:
        modified = cmp.loc['modified_latest_left']
        dset[left_site]['modified files'] = f"{modified.shape[0]} ({hsize(modified.fsize_left.sum())})"
    if 'modified_latest_right' in cmp.index:
        modified = cmp.loc['modified_latest_right']
        dset[right_site]['modified files'] = f"{modified.shape[0]} ({hsize(modified.fsize_left.sum())})"
    unique = cmp.loc['unique']
    dset[left_site]['unique files'] = f"{unique.shape[0]} ({hsize(unique.fsize_left.sum())})"
    df = pd.DataFrame(dset)
    print(f"Table 1: Summary with respect to {left_site.upper()} site\n")
    import tabulate
    print(tabulate.tabulate(df, headers='keys'))
    m = merge(left, right)
    dmap = directorymap(m)
    dmap.columns = [c.replace('left', left_site).replace('right', right_site) 
                    for c in dmap.columns]
    dmap = dmap.reset_index(drop=True)
    print("-"*70)
    print("Table 2: Common directory mapping\n")
    print(tabulate.tabulate(dmap, headers='keys'))
    #return df, dmap
