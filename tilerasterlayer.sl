#!/bin/bash
#SBATCH -A landcare00027
#SBATCH --time=2:00:00
#SBATCH --mem-per-cpu=1G
#SBATCH -o logs/tilerasterlayer.job-%j.out
#SBATCH -e logs/tilerasterlayer.job-%j.err
#SBATCH --job-name=tilerasterlayer
#SBATCH --mail-type=END
#SBATCH --mail-user=muellerm@landcareresearch.co.nz

source /landcare/sw/osgeo/osgeo.sh
python tilerasterlayer.py \
/gpfs1m/projects/landcare00034/base/raster/landsat8/2014/p72r89/landsat8_olips_p72r89_140202_flats_nztm.kea \
/gpfs1m/projects/landcare00031/data/tiled/wgt_full/landsat8_olips_p72r89_140202_flats_nztm 0 6 --verbose
