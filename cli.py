import click
import yaml
import pathlib
from pprint import pprint


with open("config.yaml") as fid:
    conf = yaml.load(fid, yaml.loader.SafeLoader)

sites = [c['site'] for c in conf]
pools = {c for item in conf for c in item['pool']}


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
    s = [c for c in conf if c['site'] == site]
    if not s:
        raise ValueError(f"Valid choices for Site: {sites}")
    s = s.pop()
    if not pool:
        print(yaml.dump(s, default_flow_style=False))
        return
    if pool not in pools:
        raise ValueError(f"Valid choices for Pool: {pools}")
    c = s['pool'][pool]
    print(yaml.dump(s, default_flow_style=False))
    return


@cli.command()
@click.option("-s", "--site", type=click.Choice(sites), required=True)
@click.option("-p", "--pool", type=click.Choice(pools), required=True)
@click.option("-f", "--filename", help="name of the run script")
def runscript(site, pool, filename):
    "makes run script for job submission"
    if site not in sites:
        raise ValueError(f"mismatch site '{site}'. Possible values: {sites}")
    if pool not in pools:
        raise ValueError(f"mismatch pool '{pool}'. Possible values: {pools}")
    c = ([c for c in conf if c["site"] == site]).pop()
    c_pool = c['pool'][pool]
    c_slurm = c['slurm']
    c_extras = c['extras']
    c_extras = "\n".join(c_extras)
    slurm_directives = process_slurm_directives(c_slurm)
    content = f"""#!/bin/bash

{slurm_directives}

{c_extras}

export POOL_SITE={site}
export POOL_NAME={pool}

python checksums.py
"""
    if not filename:
        filename = f"{pool}_{site}.sh"
    f = pathlib.Path(filename)
    with open(f, "w") as fid:
        fid.write(content)
    print(f"created '{f}'.\nsubmit to slurm as 'sbatch {f}'")

    

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

if __name__ == "__main__":
    cli()

