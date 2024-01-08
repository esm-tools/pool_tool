import os
import click
import yaml
import pathlib
from pprint import pprint


config_file = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(config_file) as fid:
    conf = yaml.safe_load(fid)

sites = list(conf)
pools = {p for site in sites for p in conf[site]["pool"]}


def process_slurm_directives(d):
    result = []
    for key, val in d.items():
        if key.strip().startswith("--"):
            if val.strip():
                s = f"#SBATCH {key}={val}"
            else:
                s = f"#SBATCH {key}"
        else:
            if val.strip():
                s = f"#SBATCH {key} {val}"
            else:
                s = f"#SBATCH {key}"
        result.append(s)
    result = "\n".join(result)
    return result


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--all",
    "showall",
    is_flag=True,
    default=False,
    help="Gets the whole config in the absence of arguments.",
)
@click.argument("site", required=False, default="")
@click.argument("pool", required=False, default="")
def config(showall, site, pool):
    "shows config information for a given site"
    if not site:
        if showall:
            print(yaml.dump(conf, default_flow_style=False))
        else:
            print(f"Possible values for SITE: {','.join(sites)}")
            print(f"Possible values for POOL: {','.join(pools)}")
        return
    if site not in sites:
        raise ValueError(f"Valid choices for Site: {sites}")
    if not pool:
        c = conf[site]
        print(yaml.dump(c, default_flow_style=False))
        return
    elif pool not in pools:
        raise ValueError(f"Valid choices for Pool: {pools}")
    c = {}
    c[site] = conf[site]
    c[site]["pool"] = {pool: conf[site]["pool"][pool]}
    print(yaml.dump(c, default_flow_style=False))
    return


@cli.command()
@click.option("-f", "--filename", help="name of the run script")
@click.argument("site", type=click.Choice(sites), required=True)
@click.argument("pool", type=click.Choice(pools), required=True)
def runscript(filename, site, pool):
    "makes run script for job submission"
    if site not in sites:
        raise ValueError(f"mismatch site '{site}'. Possible values: {sites}")
    if pool not in pools:
        raise ValueError(f"mismatch pool '{pool}'. Possible values: {pools}")
    c = conf[site]
    c_pool = c["pool"][pool]
    c_slurm = c["slurm"]
    c_ignore = ",".join(c_pool["ignore"]) if c_pool["ignore"] else ""
    c_extras = "\n".join(c["extras"])
    c_conf_str = yaml.dump(c_pool, default_flow_style=True, width=float("inf")).replace(
        "\n", ""
    )
    slurm_directives = process_slurm_directives(c_slurm)
    content = f"""#!/bin/bash

{slurm_directives}

{c_extras}

python checksums.py {c_pool['path']} --outfile {c_pool['outfile']} --ignore {c_ignore!r} 
"""
    if not filename:
        filename = f"{pool}_{site}.sh"
    f = pathlib.Path(filename)
    with open(f, "w") as fid:
        fid.write(content)
    print(f"created '{f}'.\nsubmit to slurm as 'sbatch {f}' on {site}")
    return


@cli.command()
@click.option(
    "-o", "--outfile", type=click.File("w"), default="-", help="file to write results"
)
@click.option(
    "--format",
    "fileformat",
    type=click.Choice(["csv", "yaml"]),
    default="csv",
    help="output format (default: csv)",
)
@click.option(
    "--fullpath",
    is_flag=True,
    required=False,
    help="displays full path instead of relative path",
)
@click.option("--ignore", help="ignores directory and files")
@click.argument("left", required=True, type=click.Path())
@click.argument("right", required=True, type=click.Path())
def compare(outfile, fileformat, fullpath, ignore, left, right):
    """Compare csv files containing checksum to infer the status of data
    in these data pools. The results include, synced files at both HPC sites. unsynced files.
    directory mapping of synced files. filename mis-matches.

    LEFT: csv file containing checksums of all files in the pool for a given project and HPC site.

    RIGHT: similar file as LEFT but from different HPC site for the same project.
    """
    from .analyse import read_csv, compare_compact, compare_directory_view

    ld, ld_dups = read_csv(left, ignore=ignore)
    lr, lr_dups = read_csv(right, ignore=ignore)
    if fileformat == "yaml":
        d = compare_directory_view(ld, lr, fullpath=fullpath)
        click.echo(f"Writing results as yaml to file {outfile.name}")
        yaml.dump(d, outfile, sort_keys=False)
        return
    columns = "rpath"
    if fullpath:
        columns = "fpath"
    res = compare_compact(ld, lr, columns=columns, relabel=True)
    if "stdout" in outfile.name:
        click.echo(res)
    else:
        click.echo(f"Writing results as csv to file {outfile.name}")
        res.to_csv(outfile)
        click.echo(res)


@cli.command()
@click.option("--ignore", help="ignores directory and files")
@click.argument("left", required=True, type=click.Path())
@click.argument("right", required=True, type=click.Path())
def summary(ignore, left, right):
    """Prints a short summary by analysing csv files.

    LEFT: csv file containing checksums of all files in the pool for a given project and HPC site.

    RIGHT: similar file as LEFT but from different HPC site for the same project.
    """
    from .analyse import summary

    summary(left, right, ignore)


@cli.command()
@click.option("--ignore", help="ignores directory and files")
@click.argument("left", required=True, type=click.Path())
@click.argument("right", required=True, type=click.Path())
def recommandations(ignore, left, right):
    """Provides suggestion for preparing the target site pool directory for sync operations.

    LEFT: csv file containing checksums of all files in the pool for a given project and HPC site.

    RIGHT: similar file as LEFT but from different HPC site for the same project.
    """
    from .analyse import read_csv, compare_compact, directory_map, merge

    print(
        "\nWARNING: Please be cautioned that this tool bears *NO* resposibility for any of the outcomes by executing the following actions.\n"
    )
    ld, ld_dups = read_csv(left, ignore=ignore)
    rd, rd_dups = read_csv(right, ignore=ignore)
    _, left_pool, left_site = left.split("_")
    left_site, _ = os.path.splitext(left_site)
    _, right_pool, right_site = right.split("_")
    right_site, _ = os.path.splitext(right_site)
    m = merge(ld, rd)
    d = directory_map(m)
    if not d.empty:
        # print(d)
        symlinks = []
        pdirs = set()
        prefix = rd.prefix.iloc[0]
        print(f"\nOn {rd.site.upper()} site, do the following\n")
        for row in d.itertuples():
            if row.rparent_left != row.rparent_right:
                symlinks.append(
                    f"ln -s {os.path.join(prefix, row.rparent_right)} {os.path.join(prefix, row.rparent_left)}"
                )
                pdirs.add(os.path.dirname(row.rparent_left))
        if pdirs:
            print(
                "Create the following directories (prep'ing for symlink, construct intermediate directories)\n"
            )
            for _pdir in sorted(pdirs):
                print(f"mkdir -p {os.path.join(prefix, _pdir)}")
            print("\nNow create the symlinks (renaming instead of linking?) \n")
            for ln in symlinks:
                print(ln)
    cmp = compare_compact(ld, rd, columns="fpath")
    index = cmp.index.unique()
    modified = []
    if "modified_latest_right" in index:
        for row in cmp.loc["modified_latest_right"].itertuples():
            line = f"mv {row.fpath_right} {row.fpath_right}_new"
            modified.append(line)
    if "modified_latest_left" in index:
        for row in cmp.loc["modified_latest_left"].itertuples():
            line = f"cp {row.fpath_right} {row.fpath_right}_old"
            modified.append(line)
    if modified:
        print(
            "\nFound some modified files. To prevent them from getting over-written in a sync operation, rename them as follows\n"
        )
        for line in modified:
            print(line)
        print(
            "\nPlease note that if files on the target site are newer compared to source site, they are suffixed with '_new'\n"
        )
        print(
            f"Now perform the rsync operation as directory structre on {rd.site.upper()} is complient to {ld.site.upper()}\n"
        )
        print(f"rsync -avz {ld.site}:{ld.prefix.iloc[0]} {rd.site}:{rd.prefix.iloc[0]}")
        print(
            "\nBeware of git repositories in the pools, exclude them in rsync command to preserve history if desired.\n"
        )
        print("\n")


if __name__ == "__main__":
    cli()
