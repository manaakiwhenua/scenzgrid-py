#!/bin/bash
#SBATCH -A landcare00027
#SBATCH --time=8:00:00
#SBATCH --mem-per-cpu=1G
#SBATCH -o /projects/landcare00031/logs/createcube.job-%j.out
#SBATCH -e /projects/landcare00031/logs/createcube.job-%j.err
#SBATCH --job-name=createcube
#SBATCH --mail-type=END
#SBATCH --mail-user=muellerm@landcareresearch.co.nz

source /landcare/sw/osgeo/osgeo.sh
python createcube.py /gpfs1m/projects/landcare00031/data/tiled/nz \
/projects/landcare00031/data/cubetest/nz p72r89,p73r90 0 6 \
--parallelism slurm
