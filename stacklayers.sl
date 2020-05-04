#!/bin/bash
#SBATCH -A landcare00031
#SBATCH --time=12:00:00
#SBATCH --mem-per-cpu=1G
#SBATCH -o logs/stacklayers.job-%j.out
#SBATCH -e logs/stacklayers.job-%j.err
#SBATCH --job-name=stacklayers
#SBATCH --mail-type=END
#SBATCH --mail-user=muellerm@landcareresearch.co.nz

source /landcare/sw/osgeo/osgeo.sh

python stacklayers.py /gpfs1m/projects/landcare00031/data/tiled/christchurch /projects/landcare00031/data/cube/chch_new 6 6 \
--shapefile /projects/landcare00031/data/AOIs/linzcube-roi_christchurch.shp --parallelism slurm --verbose
