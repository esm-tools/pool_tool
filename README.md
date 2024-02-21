# Ptool

A tool to manage pools across different sites. With Ptool it is possible to take
snap-shot of a pool at regular intervals and compare them to monitor the changes
to the pool overtime. These snap-shots can be from the same machine or from
different machines. It also provides tools to sync the state between pool on
different machines.

## Installation

As this a private repository, the easiest and usual approach of installing via
pip `pip install ptool` is not made available.  The current approach is to clone
the repository on the machine where Ptool needs to installed

``` shell
git clone https://github.com/esm-tools/pool_tool.git
cd pool_tool
```

Create a virtual environment, either using `conda` or `pyvenv` to install the package.

### Using Conda

``` shell
conda env create -f environment.yml
conda activate ptool
pip install .
```

### Usage

`Ptool` provides 4 commands to manage the pool.
  - `checksums` to create snap-shot of the pool
  - `summary` to get an overview by comparing 2 snap-shots
  - `compare` to write concrete results of comparing 2 snap-shots
  - `prepare-rsync` produces script to transfer files from one machine to another
  
#### `checksums` (snap-shot of pool)

Getting the help text to see the all the options

``` shell
$ ptool checksums --help
Usage: ptool checksums [OPTIONS] PATH

  Calculates imohash checksum of file(s) at the given path. Results are
  presented as csv.

Options:
  --drop-hidden-files / --no-drop-hidden-files
                                  ignore hidden files  [default: drop-hidden-
                                  files]
  --ignore TEXT                   ignore dirs or files
  -o, --outfile FILENAME          output filename
  --help                          Show this message and exit.
```

Lets say, `Ptool` is installed on Levante and the pool to take snap-shot is
`fesom2` project, then invoke `checksum` as follows:

``` shell
$ ptool checksums --drop-hidden-files --ignore dist_ -o levante_fesom2.csv /pool/data/AWICM/FESOM2
Gathering files...
skipping.. /pool/data/AWICM/FESOM2/FORCING/ERA5 -> /mnt/lustre01/work/ba1138/a270099/era5/forcing/inverted
getting files Elapsed 0.71s
nfiles: 3031
Calculating hashes...
100%|████████████████████████████████████████████████████| 3031/3031 [00:02<00:00, 1136.25files/s]
calculating hashes Elapsed 3.32s
Writing results to levante_fesom2.csv
```

#### Remote checksums

It is also possible to get the `checksums` of pool on the remote site. Lets say
we are on Albedo machine and want to compare snap-shot for the project `fesom2`
from both Albedo and Levante then we can also directly invoke `checksums`
command on Levante from Albedo using ssh command as follows:

``` shell
$ ssh a270243@levante.dkrz.de "~/miniforge3/envs/ptool/bin/ptool checksums /pool/data/AWICM/FESOM2 --drop-hidden-files --ignore dist_" > levante_fesom2.csv
Gathering files...
skipping.. /pool/data/AWICM/FESOM2/FORCING/ERA5 -> /mnt/lustre01/work/ba1138/a270099/era5/forcing/inverted
getting files Elapsed 0.98s
nfiles: 3031
Calculating hashes...
100%|██████████| 3031/3031 [00:03<00:00, 981.43files/s]
calculating hashes Elapsed 3.85s
Writing results to <stdout>
```

In the above command, since there is no `-o/--outfile` option provided, the
results are written to `<stdout>` which is piped to a local file on Albedo. It
is also certainly possible to write the snap-shot results to a file on Levante
and then copy it over to Albedo. It is up-to the user, how they want to trigger
the computation but the main point here is the user is supposed to gather the
snap-shots on to the machine where further analysis is carried out.

NOTE:
User is free to choose any meaningful name for the csv file as they see fit. The
same name is used in displaying the results in the analysis part.

#### Summary

Lets say we have computed the snap-shot for the project pool `fesom2` on both Levante and Albedo, then invoke `summary` as follows to get a quick overview of the states these pool are in

``` shell
$ ptool summary --compact levante_fesom2.csv albedo_fesom2.csv

Table 1: Summary with respect to LEVANTE_FESOM2

                 levante_fesom2            albedo_fesom2
---------------  ------------------------  -------------------
pool             FESOM2                    FESOM2
checksum file    levante_fesom2.csv        albedo_fesom2.csv
prefix           /pool/data/AWICM/FESOM2/  /albedo/pool/FESOM2
files            3031 (29.8 TB)            1577 (1.1 TB)
duplicate files  56 (615.2 MB)             167 (5.6 GB)
identical files  547 (44.8 GB)             547 (44.8 GB)
unique files     2458 (29.8 TB)            nan
modified files   nan                       9 (17.5 MB)
----------------------------------------------------------------------

Table 2: Common directory mapping

    rparent_levante_fesom2    rparent_albedo_fesom2
--  ------------------------  ----------------------------
 0  FORCING/CORE2             /forcing/CORE2
 1  INITIAL/phc3.0            /hydrography_dsidoren/phc3.0
 2  MESHES/CORE2              /core2_meanz_broken
 3  MESHES/CORE2/figures      /core2_meanz_broken/figures
 4  MESHES_FESOM2.1/core2     /core2
 5  MESHES_FESOM2.1/hr        /HR
 6  MESHES_FESOM2.1/mr        /mr
----------------------------------------------------------------------

Table 3: LEVANTE_FESOM2 perspective, per directory associations

                       modified      identical  unique      total
---------------------  ----------  -----------  --------  -------
MESHES_FESOM2.1/hr     1                    14  1              16
MESHES_FESOM2.1/mr     1                    14  1              16
MESHES_FESOM2.1/core2  7                    18  11             36
FORCING/CORE2          -                   479  -             479
INITIAL/phc3.0         -                     5  -               5
MESHES/CORE2/figures   -                     8  -               8
MESHES/CORE2           -                     9  -               9
----------------------------------------------------------------------
```

There are more options available for `summary` command to alter the results. Use
the `ptool summary --help` to see and investigate other options.

#### comapre

To get the specifics of the per-files associations, use the compare command as
follows

``` shell
$ ptool compare -o lev_alb_fesom2_cmp.csv levante_fesom2.csv albedo_fesom2.csv 
Writing results as csv to file lev_alb_fesom2_cmp.csv
                                          rpath_left                         rpath_right
flag                                                                                    
identical          FORCING/CORE2/ncar_precip.1948.nc  /forcing/CORE2/ncar_precip.1948.nc
identical          FORCING/CORE2/ncar_precip.1952.nc  /forcing/CORE2/ncar_precip.1952.nc
identical          FORCING/CORE2/ncar_precip.1955.nc  /forcing/CORE2/ncar_precip.1955.nc
identical          FORCING/CORE2/ncar_precip.1953.nc  /forcing/CORE2/ncar_precip.1953.nc
identical          FORCING/CORE2/ncar_precip.1956.nc  /forcing/CORE2/ncar_precip.1956.nc
...                                              ...                                 ...
unique     FORCING/era5/forcing/inverted/t2m.1972.nc                                 NaN
unique     FORCING/era5/forcing/inverted/t2m.1940.nc                                 NaN
unique     FORCING/era5/forcing/inverted/t2m.1948.nc                                 NaN
unique     FORCING/era5/forcing/inverted/t2m.1960.nc                                 NaN
unique     FORCING/era5/forcing/inverted/t2m.1956.nc                                 NaN

[3014 rows x 2 columns]
```

The comparison results are written to `lev_alb_fesom2_cmp.csv` file. Explore the
contents this file using your favorite editor or search for specific entries
using the `grep` command. For instance, using `grep` to finding the occurrences
of folder `MESHES_FESOM2.1/hr`

``` shell
$ grep MESHES_FESOM2.1/hr lev_alb_fesom2_cmp.csv
identical,MESHES_FESOM2.1/hr/edgenum.out,/HR/edgenum.out
identical,MESHES_FESOM2.1/hr/elvls.out,/HR/elvls.out
identical,MESHES_FESOM2.1/hr/hr_griddes_elements.nc,/HR/hr_griddes_elements.nc
identical,MESHES_FESOM2.1/hr/hr_zaxis.txt,/HR/hr_zaxis.txt
identical,MESHES_FESOM2.1/hr/nlvls.out,/HR/nlvls.out
identical,MESHES_FESOM2.1/hr/elem2d.out,/HR/elem2d.out
identical,MESHES_FESOM2.1/hr/hr_griddes_elements_IFS.nc,/HR/hr_griddes_elements_IFS.nc
identical,MESHES_FESOM2.1/hr/nod2d.out,/HR/nod2d.out
identical,MESHES_FESOM2.1/hr/fesom.mesh.diag.nc,/HR/fesom.mesh.diag.nc
identical,MESHES_FESOM2.1/hr/aux3d.out,/HR/aux3d.out
identical,MESHES_FESOM2.1/hr/hr_griddes_nodes.nc,/HR/hr_griddes_nodes.nc
identical,MESHES_FESOM2.1/hr/hr_griddes_nodes_IFS.nc,/HR/hr_griddes_nodes_IFS.nc
identical,MESHES_FESOM2.1/hr/edge_tri.out,/HR/edge_tri.out
identical,MESHES_FESOM2.1/hr/edges.out,/HR/edges.out
modified_latest_right,MESHES_FESOM2.1/hr/README.md,/HR/README.md
unique,MESHES_FESOM2.1/hr/README,
```

#### prepare-rsync

This command produces a shell script which contains a list of rsync commands to
be executed. Before running the shell script, it is recommended to check the
contents of this file to see if `prepare-rsync` has produced the desired
result. User can directly manipulate the shell script to adjust for minor
artifacts in-case the options offered by the command does not yield the exact
result the user is expecting. Please check out the `--help` command for details
on the options along with few examples of invoking this command.

``` shell
$ ptool prepare-rsync --help
Usage: ptool prepare-rsync [OPTIONS] LEFT RIGHT

  Prepares rsync commands for the transfer.

  Denpending on where data needs to pushed or pulled, provide either
  `--lefthost` or `--righthost` information to prefix that path.

  Note: when Albedo system is invloved, run this command on Albedo and provide
  the other host information as Albedo can not be reached from other machines.

  Examples that WORK:

  # commands executed on Albedo (i.e., we are on Albedo)

  1. sync data: Levante -> Albedo

     ptool prepare-rsync --lefthost user@levante.dkrz.de
     checksum_levante_fesom2.csv checksum_albedo_fesom2.csv

  2. sync data: Albedo -> Levante

     ptool prepare-rsync --righthost user@levante.dkrz.de
     checksum_albedo_fesom2.csv checksum_levante_fesom2.csv

  Examples that FAIL:

  # commands executed on Levante (i.e., we are on Levante)

  1. sync data: Levante -> Albedo

     ptool prepare-rsync --righthost user@albedo0.dmawi.de
     checksum_levante_fesom2.csv checksum_albedo_fesom2.csv

     will produce rsync commands as follows:

     rsync /some/path/on/levante user@albedo0.dmawi.de:/some/path/on/albedo

     Although syntactically correct command, it fails as Albedo is not
     reachable from other machines

  2. sync data: Albedo -> Levante

     ptool prepare-rsync --lefthost user@albedo0.dmawi.de
     checksum_albedo_fesom2.csv checksum_levante_fesom2.csv

Options:
  --ignore TEXT                   ignores directory and files
  --flags [unique|modified|both]  association type to include
  -t, --threshold FLOAT           minumin value to satisfy valid association
                                  [default: 0.1]
  -l, --lefthost TEXT             username@host prefix to the path for left
                                  file
  -r, --righthost TEXT            username@host prefix to the path for right
                                  file
  --help                          Show this message and exit.
```

Assuming we are on Albedo and want to transfer the files from Levante to Albedo,
invoke the `prepare-rsync` command as follows:

``` shell
$ ptool prepare-rsync --lefthost a270243@levante.dkrz.de levante_fesom2.csv albedo_fesom2.csv 
Created sync_cmd.sh
```

Verify the contents of `sync_cmd.sh` before executing the script. It is also
possible to directly edit this file to remove selected files from the
transaction.

To give a sneak-peak into the `sync_cmd.sh` file, looking for
`MESHES_FESOM2.1/hr` entry as follows:

``` shell
$ grep MESHES_FESOM2.1/hr sync_cmd.sh
# MESHES_FESOM2.1/hr
rsync -av --files-from=flist/95087e3b a270243@levante.dkrz.de:/pool/data/AWICM/FESOM2/MESHES_FESOM2.1/hr/ /albedo/pool/FESOM2/HR/
```
