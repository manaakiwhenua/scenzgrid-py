#!/usr/bin/env python
"""
Combines n rHEALPix-tiled layers into one (thereby creating a data cube)

"""
# This file is part of scenzgrid-py
# Copyright (C) 2014 Markus U. Mueller (muellerm AT landcareresearch DOT co DOT nz)
# Copyright (C) 2014 Robert Gibb (gibbr AT landcareresearch DOT co DOT nz)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import time
import numpy
import shutil
import argparse
import glob

import osgeo.gdal as gdal
from osgeo import ogr
from rios import applier

from rhealpix_dggs.dggs import *
from rhealpix_dggs.ellipsoids import *

from slurm_utils import *
from data_utils import *



if __name__ == '__main__':
    start = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("indir", type = str, help="Specify the input directory including rHEALPix tiled datasets.")
    parser.add_argument("outdir", type = str, help="Specify the output directory.")
    parser.add_argument("minres", type = int, help="Specify the minimum output grid resolution.")
    parser.add_argument("maxres", type = int, help="Specify the maximum output grid resolution.")
    parser.add_argument("-g", "--globalex", help="Create global coverage output (default: False).", action ="store_true")
    parser.add_argument("-s", "--shapefile", help="Supply shapefile that defines output extent")
    parser.add_argument("-p", "--parallelism", help="Choice of no (default) or slurm")
    parser.add_argument("-v", "--verbose", help="Show debug messages (default: False).", action ="store_true")

    args = parser.parse_args()

    if args.indir == None:
        print("Error: no input file specified")
        sys.exit()
    indir = args.indir    

    if args.outdir == None:
        print("Error: no output directory specified")
        sys.exit()
    outfileroot = args.outdir

    if args.minres == None:
        print("Error: minres not specified")
        sys.exit()
    minresolution = int(args.minres)

    if args.maxres == None:
        print("Error: yres not specified")
        sys.exit()
    maxresolution = int(args.maxres)

    if args.globalex:
        globalextent = True
    else:
        globalextent = False
    
    if args.shapefile:
        shapefile = str(args.shapefile)
    else:
        shapefile = None

    if args.parallelism == 'slurm':
        parallelism = args.parallelism
    else:
        parallelism = None

    if args.verbose:
        debug = True
    else:
        debug = False

    # 'wipe clean' and create new data cube directory
    if os.path.exists(outfileroot):
        shutil.rmtree(outfileroot)
        os.makedirs(outfileroot)

    # create 'standard' DGGS based on WGS84 and center meridian at 52 deg and corresponding WKT string
    n_square = 1
    s_square = 3
    a = 6378137
    central_meridian = 52
    E = Ellipsoid(lon_0 = central_meridian)
    rddgs = RHEALPixDGGS(ellipsoid = E, north_square=n_square, south_square=s_square, N_side=3)
    
    if globalextent:
        nw = [-180, 90]
        se = [180, -90]
    elif shapefile:
        # Get a Layer's Extent; shapefile has to use lat/long
        inShapefile = shapefile
        inDriver = ogr.GetDriverByName("ESRI Shapefile")
        inDataSource = inDriver.Open(inShapefile, 0)
        inLayer = inDataSource.GetLayer()
        ext = inLayer.GetExtent()
        nw = [ext[0], ext[3]]
        se = [ext[1], ext[2]]
    else: # use NZ coastline as default
        inShapefile = '/projects/landcare00031/data/AOIs/nzcoast.shp'
        inDriver = ogr.GetDriverByName("ESRI Shapefile")
        inDataSource = inDriver.Open(inShapefile, 0)
        inLayer = inDataSource.GetLayer()
        ext = inLayer.GetExtent()
        nw = [ext[0], ext[3]]
        se = [ext[1], ext[2]]
        
    if parallelism == 'slurm': jobs = []  # initialize job list

    for i in range(maxresolution,minresolution-1,-1): # iterate over resolutions and create grids
        grid = rddgs.cells_from_region(i, nw, se, plane=False)
        for row in grid:
            for c in row:
                cmd = "python stacktile.py %s %s %s" %(c, indir, outfileroot)
                if debug: print(cmd)
                if parallelism == 'slurm':
                    if debug: print('Command will be submitted to SLURM')
                    jobs = submitSLURMjob(cmd, jobs)
                else:
                    if debug: print('Command will use serial processing')
                    os.system(cmd)
            if parallelism == 'slurm': jobs = checkSLURMjobs(jobs, debug = True)
    
    elapsed = time.time() - start
    print('Elapsed time (stacklayers): %g seconds' %(elapsed))
        



