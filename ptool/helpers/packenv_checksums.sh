#!/bin/bash

# works on Linux

hasconda="false"

if [ ! -z "$CONDA_EXE" ]; then
    condabin=$CONDA_EXE
    hasconda="true"
else
    condapath="${HOME}/miniconda"
    wget -O Miniforge3.sh "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
    bash Miniforge3.sh -b -p "${condapath}"
    source "${condapath}/etc/profile.d/conda.sh"
    #conda activate
    condabin="$CONDA_EXE"
fi

echo "$condabin"

echo "creating conda environments"
conda create -y -n cs python=3.10
conda activate cs
python -m pip install imohash click
conda deactivate
conda create -y -n condapack -c conda-forge python=3.10 conda-pack
conda activate condapack
echo "creating tar file of environment"
conda pack -n cs -o cs.tar.gz
echo "created file: cs.tar.gz"

echo "cleaning up"

conda deactivate
conda remove -y -n cs --all
conda remove -y -n condapack --all

if [ $hasconda = "false" ]; then
    rm Miniforge3.sh
    conda init --reverse
    CONDA_BASE_ENV=$(conda info --base)
    rm -rf ${CONDA_BASE_ENV}
    #rm .condarc
    #rm -rf ${HOME}/.conda
fi

echo "DONE cleaning up"
echo "transfer cs.tar.gz to HPC machine and unpack by running..."
echo "mkdir cs "
echo "tar -xzf cs.tar.gz -C cs "
echo "for sanity check, try the following..."
echo "./cs/bin/python --version"
