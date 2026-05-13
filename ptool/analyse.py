import os
import itertools
import pandas as pd
import numpy as np
import humanize
from collections import defaultdict

__all__ = [
    "read_csv",
    "compare",
    "compare_compact",
    "compare_directory_view",
    "summary",
    "merge",
    "directory_map",
]


class _SnapshotFrame(pd.DataFrame):
    """DataFrame subclass that carries snapshot metadata through pandas operations."""

    _metadata = ["site", "filename"]

    @property
    def _constructor(self):
        return _SnapshotFrame


def read_csv(filename, ignore=None, drop_duplicates=False):
    filename = os.path.expanduser(filename)
    df = _SnapshotFrame(pd.read_csv(filename, engine="pyarrow"))
    df = df.rename(columns={"fname": "fpath", "md5": "checksum"})
    df = df[df.checksum != "-"]
    df["fname"] = df.fpath.apply(os.path.basename)
    df["prefix"] = os.path.commonpath(list(df.fpath.apply(os.path.dirname)))
    df["rpath"] = df.fpath.str.removeprefix(df.prefix[0])
    df["rparent"] = df.rpath.apply(os.path.dirname)
    for name, dtype in df.dtypes.items():
        if dtype == "object":
            df[name] = df[name].astype("string[pyarrow]")
    if ignore:
        df = df[~df.rparent.str.contains(ignore)]
        df = df[~df.fname.str.contains(ignore)]
    df = df.sort_values(by=["checksum", "mtime"])
    dups = df[df.duplicated(subset=["checksum", "fname"]).values]
    if drop_duplicates:
        df = df.drop_duplicates(subset=["checksum", "fname"])
    df["mtime"] = pd.to_datetime(df["mtime"], unit="s")
    df.filename = filename
    df.site = os.path.splitext(os.path.basename(filename))[0]
    return df, dups


def merge(dl, da, on="checksum", how="inner"):
    m = pd.merge(dl, da, on=on, how=how, suffixes=("_left", "_right"))
    # TF-IDF weighted folder association:
    # down-weight filenames that appear in many left directories (generic sequential
    # names like my_list00001.out) so they don't drive false folder pairings.
    # Files unique to one directory carry full weight; files ubiquitous across the
    # pool carry near-zero weight.  IDF = log(total_dirs / dirs_containing_fname).
    fname_col = "fname_left" if "fname_left" in m.columns else "fname"
    n_dirs = max(dl["rparent"].nunique(), 1)
    n_dirs_per_fname = dl.groupby("fname")["rparent"].nunique()
    idf = np.log(n_dirs / n_dirs_per_fname).clip(lower=0.0)
    m["_score"] = m[fname_col].map(idf).fillna(0.0)
    # For each left folder, pick the right folder with the highest TF-IDF score,
    # then repeat from the right side to enforce 1:1 pairing.
    pair_scores = m.groupby(["rparent_left", "rparent_right"])["_score"].sum().reset_index()
    best_right = pair_scores.loc[
        pair_scores.groupby("rparent_left")["_score"].idxmax(),
        ["rparent_left", "rparent_right"],
    ]
    mm = m.merge(best_right, on=["rparent_left", "rparent_right"])
    pair_scores2 = mm.groupby(["rparent_left", "rparent_right"])["_score"].sum().reset_index()
    best_left = pair_scores2.loc[
        pair_scores2.groupby("rparent_right")["_score"].idxmax(),
        ["rparent_left", "rparent_right"],
    ]
    mm = mm.merge(best_left, on=["rparent_left", "rparent_right"])
    mm = mm.drop(columns=["_score"])
    return mm


def directory_map(m):
    return (m[["rparent_left", "rparent_right"]]).drop_duplicates()


def _suspicious_pairs(cmp):
    """Return (rparent_left, rparent_right) pairs where every matched file is
    modified and none are identical.

    When two directories share filename patterns but hold different data (e.g.
    MPI partition files for different core counts), all files appear as modified
    and zero are identical. That pattern is a reliable indicator that the folder
    mapping is spurious rather than a genuine sync mismatch.
    """
    c = cmp.reset_index()
    paired = c[c["flag"] != "unique"]
    bad = set()
    for rparent_left, group in paired.groupby("rparent_left"):
        flags = set(group["flag"].unique())
        has_modified = bool(flags & {"modified_latest_left", "modified_latest_right"})
        if has_modified and "identical" not in flags:
            for rp_right in group["rparent_right"].dropna().unique():
                bad.add((rparent_left, str(rp_right)))
    return bad


def compare(left, right, relabel=False, threshold=0.1):
    by_hash = merge(left, right)
    by_name = merge(left, right, on="fname")
    by_hash["flag"] = ""
    by_name["flag"] = ""
    common_hashes = list(by_hash.checksum)
    common_names = list(by_name.checksum_left)
    common_cs = list(set(common_hashes + common_names))
    renamed_mask = by_hash.fname_left != by_hash.fname_right
    renamed_df = by_hash[renamed_mask]
    results = {}
    ## identify renamed files (checksum matches but not filename)
    if not renamed_df.empty:
        renamed_df = renamed_df.copy()
        renamed_df["flag"] = "renamed"
        renamed_df.set_index("flag", inplace=True)
        renamed_df["checksum_right"] = renamed_df["checksum"]
        renamed_df.rename(columns={"checksum": "checksum_left"}, inplace=True)
        results["renamed"] = renamed_df
        by_hash = by_hash[~renamed_mask]
    ## identify identical files (both checksum and filename match)
    by_hash = by_hash.copy()
    by_hash["flag"] = "identical"
    by_hash.set_index("flag", inplace=True)
    by_hash["checksum_right"] = by_hash["checksum"]
    by_hash.rename(columns={"checksum": "checksum_left"}, inplace=True)
    results["identical"] = by_hash
    ## identify modified files (filename matches but not checksum)
    ## latest mtime in the file-pair is the most recent one
    ## This means, indicating which of the pairs is latest is useful
    modified = by_name[by_name.checksum_left != by_name.checksum_right]
    left_latest = modified[modified.mtime_left > modified.mtime_right]
    if not left_latest.empty:
        left_latest = left_latest.copy()
        left_latest["flag"] = "modified_latest_left"
        left_latest.set_index("flag", inplace=True)
        left_latest["fname_right"] = left_latest["fname"]
        left_latest.rename(columns={"fname": "fname_left"}, inplace=True)
        results["modified_latest_left"] = left_latest
    right_latest = modified[modified.mtime_left < modified.mtime_right]
    if not right_latest.empty:
        right_latest = right_latest.copy()
        right_latest["flag"] = "modified_latest_right"
        right_latest.set_index("flag", inplace=True)
        right_latest["fname_right"] = right_latest["fname"]
        right_latest.rename(columns={"fname": "fname_left"}, inplace=True)
        results["modified_latest_right"] = right_latest
    ## unique files (files found only on left site (i.e., first argument))
    left_only = left[~left.checksum.isin(common_cs)]
    if not left_only.empty:
        left_only = left_only.copy()
        left_only["flag"] = "unique"
        left_only.set_index("flag", inplace=True)
        cols = [c + "_left" for c in left_only.columns]
        left_only.columns = cols
        results["unique"] = left_only
    order = {
        "identical": 1,
        "renamed": 2,
        "modified_latest_left": 3,
        "modified_latest_right": 4,
        "unique": 5,
    }
    results = pd.concat([results[key] for key in sorted(results, key=order.get)])
    if threshold:
        results = _correct_false_positive(results, threshold=threshold)
    # The following drop_duplicates makes sure that there is
    # always 1:1 mapping of files. More precisely it eliminates
    # many:1 mapping.
    results = results.drop_duplicates("rpath_left")
    if relabel:
        newcols = [
            c.replace("left", left.site).replace("right", right.site)
            for c in results.columns
        ]
        results.columns = newcols
    return results


def _correct_false_positive(df, threshold=0.1):
    dfs = []
    partial_dfs = []
    right_cols = [c for c in df.columns if c.endswith("right")]
    # default = df.loc['unique'][right_cols].iloc[0].values
    for name, grp in df.groupby("rparent_left"):
        val_counts = dict(grp.index.value_counts())
        total_count = sum(val_counts.values())
        if "unique" in val_counts:
            val_counts.pop("unique")
        partial_count = sum(val_counts.values())
        if partial_count and partial_count <= threshold * total_count:
            grp.index = [
                "unique",
            ] * grp.index.size
            grp[right_cols] = ""
            partial_dfs.append(grp)
        else:
            dfs.append(grp)
    df = pd.concat(dfs)
    if partial_dfs:
        df = pd.concat(
            [
                df,
            ]
            + partial_dfs
        )
    with pd.option_context("future.no_silent_downcasting", True):
        df = df.replace(r"^\s*$", np.nan, regex=True)
    df.index = df.index.set_names("flag")
    return df


def compare_compact(left, right, columns="rpath", threshold=0.1, relabel=False):
    df = compare(left, right, threshold=threshold)
    if isinstance(columns, str):
        columns = columns.split(",")
    cols = []
    for col in columns:
        cols.append(col + "_left")
    for col in columns:
        cols.append(col + "_right")
    df = df[cols]
    if relabel:
        df = df[cols]
        newcols = [
            c.replace("left", left.site).replace("right", right.site)
            for c in df.columns
        ]
        df.columns = newcols
    return df


def compare_directory_view(left, right, fullpath=False, relabel=True):
    c = compare(left=left, right=right)
    c.reset_index(inplace=True)
    dm = defaultdict(dict)
    correct_gname = lambda x: x
    if relabel:
        correct_gname = lambda x: x.replace("left", "first").replace("right", "second")
    if fullpath:
        c["fparent_left"] = c["fpath_left"].apply(os.path.dirname)
        groupkey = "fparent_left"
    else:
        groupkey = "rparent_left"
    for gname, group in c.groupby(groupkey):
        total_files = group.shape[0]
        summary = {
            correct_gname(_gname): f"{_g.shape[0]}/{total_files}"
            for _gname, _g in group.groupby("flag")
        }
        try:
            summary["paths"] = [
                os.path.dirname(group.fpath_left.iloc[0]),
                os.path.dirname(group.fpath_right.iloc[0]),
            ]
        except TypeError:
            summary["paths"] = os.path.dirname(group.fpath_left.iloc[0])
        dm[gname]["summary"] = summary
        d = {}
        for _gname, _g in group.groupby("flag"):
            if _gname in ("identical", "unique"):
                d[_gname] = _g.fname_left.values.tolist()
            else:
                d[correct_gname(_gname)] = (
                    _g[["fname_left", "fname_right"]]
                ).values.tolist()
        dm[gname]["details"] = d
    return dict(dm)


def summary(
    filename1,
    filename2,
    ignore=None,
    compact=False,
    drop_duplicates=False,
    threshold=0.1,
):
    left, left_dups = read_csv(
        filename1, ignore=ignore, drop_duplicates=drop_duplicates
    )
    right, right_dups = read_csv(
        filename2, ignore=ignore, drop_duplicates=drop_duplicates
    )
    # _, left_pool, left_site = filename1.split("_")
    # left_site, _ = os.path.splitext(left_site)
    # _, right_pool, right_site = filename2.split("_")
    # right_site, _ = os.path.splitext(right_site)
    left_site = left.site
    right_site = right.site
    hsize = lambda x: humanize.naturalsize(x)
    left_pool = left.prefix.iloc[0]
    left_pool = os.path.basename(left_pool.rstrip(os.path.sep))
    right_pool = right.prefix.iloc[0]
    right_pool = os.path.basename(right_pool.rstrip(os.path.sep))
    dset = {}
    dset[left_site] = {
        "pool": left_pool,
        "checksum file": filename1,
        "prefix": left.prefix.iloc[0],
        "files": f"{left.shape[0]} ({hsize(left.fsize.sum())})",
        "duplicate files": f"{left_dups.shape[0]} ({hsize(left_dups.fsize.sum())})",
    }
    dset[right_site] = {
        "pool": right_pool,
        "checksum file": filename2,
        "prefix": right.prefix.iloc[0],
        "files": f"{right.shape[0]} ({hsize(right.fsize.sum())})",
        "duplicate files": f"{right_dups.shape[0]} ({hsize(right_dups.fsize.sum())})",
    }
    cmp = compare(left, right, threshold=threshold)
    if "identical" in cmp.index:
        identical = cmp.loc[["identical"]]
        dset[left_site][
            "identical files"
        ] = f"{identical.shape[0]} ({hsize(identical.fsize_left.sum())})"
        dset[right_site][
            "identical files"
        ] = f"{identical.shape[0]} ({hsize(identical.fsize_right.sum())})"
    if "renamed" in cmp.index:
        renamed = cmp.loc[["renamed"]]
        dset[right_site][
            "renamed files"
        ] = f"{renamed.shape[0]} ({hsize(renamed.fsize_left.sum())})"
    if "modified_latest_left" in cmp.index:
        modified = cmp.loc[["modified_latest_left"]]
        dset[left_site][
            "modified files"
        ] = f"{modified.shape[0]} ({hsize(modified.fsize_left.sum())})"
    if "modified_latest_right" in cmp.index:
        modified = cmp.loc["modified_latest_right"]
        dset[right_site][
            "modified files"
        ] = f"{modified.shape[0]} ({hsize(modified.fsize_left.sum())})"
    if "unique" in cmp.index:
        unique = cmp.loc[["unique"]]
        dset[left_site][
            "unique files"
        ] = f"{unique.shape[0]} ({hsize(unique.fsize_left.sum())})"
    df = pd.DataFrame(dset)
    table_no = itertools.count(1)
    print(f"\nTable {next(table_no)}: Summary with respect to {left_site.upper()} \n")

    import tabulate

    print(tabulate.tabulate(df, headers="keys"))
    m = merge(left, right)
    # dmap = directory_map(m)
    # dmap = dmap[dmap.rparent_left != dmap.rparent_right]
    dmap = (cmp[["rparent_left", "rparent_right"]]).dropna().drop_duplicates()
    suspicious = _suspicious_pairs(cmp)
    print("-" * 70)
    if not dmap.empty:
        dmap.columns = [
            c.replace("left", left_site).replace("right", right_site)
            for c in dmap.columns
        ]
        dmap = dmap.reset_index(drop=True)
        if suspicious:
            left_col = f"rparent_{left_site}"
            right_col = f"rparent_{right_site}"
            dmap["note"] = dmap.apply(
                lambda r: "[!]" if (r[left_col], r[right_col]) in suspicious else "",
                axis=1,
            )
        print(f"\nTable {next(table_no)}: Common directory mapping\n")
        print(tabulate.tabulate(dmap, headers="keys"))
        if suspicious:
            print("\n[!] 100% modified, 0 identical — folder mapping is likely spurious.")
        print("-" * 70)

    c = cmp.reset_index()

    def correct_gname(x, flip=False):
        if "modified" in x:
            x = "modified"
        return x

    dm = {}
    for gname, group in c.groupby("rparent_left"):
        summary = {
            correct_gname(_gname): f"{_g.shape[0]}"
            for _gname, _g in group.groupby("flag")
        }
        summary["total"] = f"{group.shape[0]}"
        dm[gname] = summary
    try:
        r = (
            pd.DataFrame(dm).transpose().sort_values(["identical", "renamed"])
        )  # .fillna('-')
    except KeyError:
        r = pd.DataFrame(dm).transpose()  # .fillna('-')
    cols = r.columns
    order = {"modified": 1, "identical": 3, "renamed": 2, "unique": 4, "total": 5}
    cols = sorted(cols, key=order.get)
    r = r[cols]
    if compact:
        try:
            r = r[~r[r.columns.drop(["unique", "total"])].isna().all(axis=1)]
        except KeyError:
            pass
    print(
        f"\nTable {next(table_no)}: {left.site.upper()} perspective, per directory associations\n"
    )
    print(tabulate.tabulate(r.sort_values(by=cols[:3]).fillna("-"), headers="keys"))
    print("-" * 70)
    return r
