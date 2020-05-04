WARNING: The current version of scenzgrid.py is far from being a stable software package.
It should be understood as a proof of concept of how a data cube based on the rHEALPix 
projection and DGGS can be created.

Dependencies:
- current versions of GDAL and OGR
- kealib (https://bitbucket.org/chchrsc/kealib), installed as plugin to GDAL. KEA is a raster
image format that is based on HDF5 and is included in GDAL development versions > 2.0.0.
- rHEALPixDGGS-py (http://code.scenzgrid.org/index.php/p/rhealpixdggs-py/); python library
that implements the rHEALPix projection and Discrete Global Grid system
- RIOS (https://bitbucket.org/chchrsc/rios/overview); python package for simplified raster processing

Scenzgrid.py was developed on the NeSI Pan cluster in Auckland and uses slurm for processing,
but should also run in other environments.

tileRasterLayer.py reprojects and tiles input raster files according to rHEALPix. The results can then
be stacked using stacklayers.py.