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
    if pool is None:
        c = conf[site]
        print(yaml.dump(c, default_flow_style=False))
    elif pool not in pools:
        raise ValueError(f"Valid choices for Pool: {pools}")
    c = {}
    c[site] = conf[site]
    c[site]['pool'] = conf[site]['pool'][pool]
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
    c_pool = c['pool'][pool]
    c_slurm = c['slurm']
    c_extras = c['extras']
    c_extras = "\n".join(c_extras)
    c_conf_str = yaml.dump(c_pool)
    slurm_directives = process_slurm_directives(c_slurm)
    content = f"""#!/bin/bash

{slurm_directives}

{c_extras}

export POOL_SITE={site}
export POOL_NAME={pool}
export POOL_CONF={c_conf_str}

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
@click.option("-o", "--outdir", type=click.Path(), default=".",
              help="directory where results are to be saved")
@click.argument("left", required=True, type=click.Path())
@click.argument("right", required=True, type=click.Path())
def compare(outdir, left, right):
    """Compare csv files containing checksum to infer the status of data
    in these data pools. The results include, synced files at both HPC sites. unsynced files.
    directory mapping of synced files. filename mis-matches.
    
    LEFT: csv file containing checksums of all files in the pool for a given project and HPC site.

    RIGHT: similar file as LEFT but from different HPC site for the same project.
    """
    from analyse import read_csv, Trees
    os.makedirs(outdir, exist_ok=True)
    left_data = read_csv(left)
    right_data = read_csv(right)
    t = Trees(left_data, right_data)
    t.compare()
    basedir = pathlib.Path().cwd()
    os.chdir(outdir)
    t.report()
    os.chdir(basedir)


@cli.command()
@click.argument("left", required=True, type=click.Path())
@click.argument("right", required=True, type=click.Path())
def summary(left, right):
    """Prints a short summary by analysing csv files.

    LEFT: csv file containing checksums of all files in the pool for a given project and HPC site.

    RIGHT: similar file as LEFT but from different HPC site for the same project.
    """
    from analyse import read_csv, Trees
    left_data = read_csv(left)
    right_data = read_csv(right)
    t = Trees(left_data, right_data)
    t.compare()
    t.summary()


if __name__ == "__main__":
    cli()

