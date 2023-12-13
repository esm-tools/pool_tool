import os
import click
import yaml
import pathlib
from pprint import pprint


config_file = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(config_file) as fid:
    conf = yaml.safe_load(fid)

sites = list(conf)
pools = {p for site in sites for p in conf[site]['pool']}


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
@click.option("--all", "showall", is_flag=True, default=False,
              help="Gets the whole config in the absence of arguments.")
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
    c[site]['pool'] = {pool: conf[site]['pool'][pool]}
    print(yaml.dump(c, default_flow_style=False))
    return


@cli.command()
@click.option("-f", "--filename", help="name of the run script")
@click.option("-c", "--checksum", type=click.Choice(['md5', 'sha1', 'sha256']), required=False, default='md5')
@click.argument("site", type=click.Choice(sites), required=True)
@click.argument("pool", type=click.Choice(pools), required=True)
def runscript(filename, checksum, site, pool):
    "makes run script for job submission"
    if site not in sites:
        raise ValueError(f"mismatch site '{site}'. Possible values: {sites}")
    if pool not in pools:
        raise ValueError(f"mismatch pool '{pool}'. Possible values: {pools}")
    c = conf[site]
    c_pool = c['pool'][pool]
    c_pool['checksum'] = checksum
    print(c_pool)
    c_slurm = c['slurm']
    c_extras = c['extras']
    c_extras = "\n".join(c_extras)
    c_conf_str = yaml.dump(c_pool, default_flow_style=True, width=float("inf")).replace("\n", "")
    slurm_directives = process_slurm_directives(c_slurm)
    content = f"""#!/bin/bash

{slurm_directives}

{c_extras}

export POOL_SITE={site}
export POOL_NAME={pool}
export POOL_CONF="{c_conf_str}"

python checksums.py
"""
    if not filename:
        filename = f"{pool}_{site}.sh"
    f = pathlib.Path(filename)
    with open(f, "w") as fid:
        fid.write(content)
    print(f"created '{f}'.\nsubmit to slurm as 'sbatch {f}' on {site}")
    return


@cli.command()
@click.option("-o", "--outfile", type=click.File("w"), default="-",
              help="csv file to write results")
@click.option("--fullpath", is_flag=True, required=False, help='displays full path instead of relative path')
@click.option('--ignore', help='ignores directory and files')
@click.argument("left", required=True, type=click.Path())
@click.argument("right", required=True, type=click.Path())
def compare(outfile, fullpath, ignore, left, right):
    """Compare csv files containing checksum to infer the status of data
    in these data pools. The results include, synced files at both HPC sites. unsynced files.
    directory mapping of synced files. filename mis-matches.
    
    LEFT: csv file containing checksums of all files in the pool for a given project and HPC site.

    RIGHT: similar file as LEFT but from different HPC site for the same project.
    """
    from analyse import read_csv, compare_compact
    ld, ld_dups = read_csv(left, ignore=ignore)
    lr, lr_dups = read_csv(right, ignore=ignore)
    columns = 'rpath'
    if fullpath:
        columns = 'fpath'
    res = compare_compact(ld, lr, columns=columns, relabel=True)
    if 'stdout' in outfile.name:
        click.echo(res)
    else:
        click.echo(f'Writing results as csv to file {outfile.name}')
        res.to_csv(outfile)
        click.echo(res)


@cli.command()
@click.option('--ignore', help='ignores directory and files')
@click.argument("left", required=True, type=click.Path())
@click.argument("right", required=True, type=click.Path())
def summary(ignore, left, right):
    """Prints a short summary by analysing csv files.

    LEFT: csv file containing checksums of all files in the pool for a given project and HPC site.

    RIGHT: similar file as LEFT but from different HPC site for the same project.
    """
    from analyse import summary
    summary(left, right, ignore)


if __name__ == "__main__":
    cli()

