#!/bin/bash
#SBATCH -A landcare00027
#SBATCH --time=05:00:00
#SBATCH --mem-per-cpu=10G
#SBATCH -o /projects/landcare00031/logs/scenzgrid.job-%j.out
#SBATCH -e /projects/landcare00031/logs/scenzgrid.job-%j.err
#SBATCH --job-name=scenzgrid

source /landcare/sw/osgeo/osgeo.sh
python stacktile.py S102033 /gpfs1m/projects/landcare00031/data/tiled/nz /projects/landcare00031/data/cubetest/nz
