#!/usr/bin/env python
"""
Creates a tiled dataset based on rhealpix DGGS and projection

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
import shutil
import subprocess
import time
import argparse

from osgeo import gdal, osr, ogr
from gdalconst import *

from rios import fileinfo

from rhealpix_dggs.dggs import *
from rhealpix_dggs.ellipsoids import *

from slurm_utils import *
from data_utils import *

if __name__ == '__main__':
    
    start = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type = str, help="Specify the input kea file.")
    parser.add_argument("outdir", type = str, help="Specify the output directory.")
    parser.add_argument("minres", type = int, help="Specify the minimum output grid resolution.")
    parser.add_argument("maxres", type = int, help="Specify the maximum output grid resolution.")
    parser.add_argument("-p", "--parallelism", help="Choice of no (default) or slurm")
    parser.add_argument("-r", "--resamplingmethod", type = str, help="Specify the resampling method (default: cubic")
    parser.add_argument("-t", "--tilesize", type = int, help="Specify the output tile size (default: 729)")
    parser.add_argument("-b", "--blocksize", type = int, help="Specify the output block size (default: 243)")
    parser.add_argument("-g", "--globalex", help="Create global coverage output (default: False).", action ="store_true")
    parser.add_argument("-s", "--shapefile", help="Supply shapefile that defines output extent")
    parser.add_argument("-v", "--verbose", help="Show debug messages (default: False).", action ="store_true")
    parser.add_argument("-e", "--excludelist", help="Supply file with list of tiles to be excluded (has to match maxres).")
    
    args = parser.parse_args()

    if args.infile == None:
        print("Error: no input file specified")
        sys.exit()
    infile = args.infile

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

    if args.parallelism == 'slurm':
        parallelism = args.parallelism
    else:
        parallelism = None

    if args.resamplingmethod:
        resample = args.resamplingmethod
    else:
        resample = 'cubic'

    if args.tilesize:
        tilesize = int(args.tilesize)
    else:
        tilesize = 729

    if args.blocksize:
        blocksize = int(args.blocksize)
    else:
        blocksize = 243

    if args.globalex:
        globalextent = True
    else:
        globalextent = False

    if args.shapefile:
        shapefile = str(args.shapefile)
    else:
        shapefile = None

    if args.verbose:
        debug = True
    else:
        debug = False

    excludelist = []
    if args.excludelist:        
        # read in list with excluded tiles, e.g. ocean area
        with open(args.excludelist, mode='r', encoding='utf-8') as excludefile:
            for line in excludefile:
                excludelist.append(line.rstrip())
        print(excludelist)
    else:
        excludelist = []

    if os.path.exists(outfileroot): # delete existing files if necessary
        shutil.rmtree(outfileroot)
        os.makedirs(outfileroot)

    if parallelism == 'slurm': jobs = [] # initialize job list
  
    # get extent and input crs from raster file
    ds = gdal.Open(infile)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    ext = getExtent(gt,cols,rows)
    src_srs=osr.SpatialReference()
    prj = ds.GetProjection()
    src_srs.ImportFromWkt(prj)
    s_srs = 'EPSG:%s' %(src_srs.GetAuthorityCode('projcs'))
    if s_srs == 'EPSG:None': s_srs = 'EPSG:%s' %(src_srs.GetAuthorityCode('GEOGCS'))
    if s_srs == 'EPSG:None': s_srs = 'EPSG:4326' # If all fails assume geographic coordinates
    if debug: print(s_srs)
    # if necessary transform into lat/long
    if s_srs != 'EPSG:4326':
        tgt_srs = src_srs.CloneGeogCS()
        geo_ext=reprojectCoords(ext,src_srs,tgt_srs)
        w = min(geo_ext[0][0], geo_ext[1][0])
        e = max(geo_ext[2][0], geo_ext[3][0])
        n = max(geo_ext[0][1], geo_ext[3][1])
        s = min(geo_ext[1][1], geo_ext[2][1])
        nw = [w, n]
        se = [e, s]

    # define output extent if different from input raster
    if globalextent:
        nw = [-180, 90]
        se = [180, -90]
    if shapefile:
        # Get a Layer's Extent; shapefile has to use lat/long
        inShapefile = shapefile
        inDriver = ogr.GetDriverByName("ESRI Shapefile")
        inDataSource = inDriver.Open(inShapefile, 0)
        inLayer = inDataSource.GetLayer()
        ext = inLayer.GetExtent()
        nw = [ext[0], ext[3]]
        se = [ext[1], ext[2]]
     
    if debug: print(nw)
    if debug: print(se)

    # get nodata value of input dataset; has to be set explicitely for gdalwarp
    info = fileinfo.ImageInfo(infile)
    dstnodata = info.nodataval
    print(dstnodata[0])
    if dstnodata[0] is None:
        print("nodata value for input dataset not defined. Using 0")
        dstnodata = [255]
    
    # create 'standard' DGGS based on WGS84 and center meridian at 52 deg and corresponding WKT string
    n_square = 1
    s_square = 3
    a = 6378137
    central_meridian = 52
    E = Ellipsoid(lon_0 = central_meridian)
    rddgs = RHEALPixDGGS(ellipsoid = E, north_square=n_square, south_square=s_square, N_side=3)
    t_srs = "\'+proj=rhealpix +lon_0=%f +a=%f +ellps=WGS84 +north_square=%d +south_square=%d +towgs84=0,0,0 +wktext\'" %(central_meridian, a, n_square, s_square) # WKT string for rhealpix 

    for i in range(maxresolution,minresolution-1,-1): # iterate over resolutions and create grids
        grid = rddgs.cells_from_region(i, nw, se, plane=False)
        for row in grid:
            for c in row:
                if debug: print(str(c))
                filepath = os.path.join(outfileroot, getFilePath(c))
                if debug: print(filepath)
                if not os.path.exists(os.path.dirname(filepath)):
                    if debug: print('now will create %s' %(os.path.dirname(filepath)))
                    os.makedirs(os.path.dirname(filepath))                
                # Extract coordinates needed for tiling
                vertices = c.vertices()                
                xmin=vertices[0][0]
                ymin=vertices[2][1]
                xmax=vertices[1][0]
                ymax=vertices[0][1]                 
                if i == maxresolution:  # grid with highest resolution is created from original data
                    if str(c) not in excludelist:
                        # Following parameters should to be used for global datasets (EPSG:4326): -wo "SAMPLE_STEPS=168" -wo "SAMPLE_GRID=YES"
                        warpstring = 'gdalwarp -dstnodata %s -s_srs %s -t_srs %s -te %f %f %f %f -ts %d %d -r %s -co IMAGEBLOCKSIZE=%d -of kea %s %s' \
                        %(int(dstnodata[0]), s_srs, t_srs,  xmin, ymin, xmax, ymax, tilesize, tilesize, resample, blocksize, infile, filepath)
                    else: warpstring = False
                else: #create lower resolution grids from higher ones by resampling
                    subcellstr = ''
                    for cell in c.subcells():
                        appendstring = os.path.join(outfileroot, getFilePath(cell))
                        if os.path.isfile(appendstring):
                            subcellstr = subcellstr + ' ' + appendstring
                    if not subcellstr:
                        warpstring = False
                    else:
                        warpstring = 'gdalwarp -dstnodata %s -s_srs %s -t_srs %s -te %f %f %f %f -ts %d %d -r %s -co IMAGEBLOCKSIZE=%d -of kea %s %s' \
                            %(dstnodata[0], t_srs, t_srs, xmin, ymin, xmax, ymax, tilesize, tilesize, resample, blocksize, subcellstr, filepath)
                if debug: print(warpstring)
                if warpstring: 
                    if  parallelism == 'slurm':
                        jobs = submitSLURMjob(warpstring, jobs)
                    else: os.system(warpstring)

        if parallelism == 'slurm': jobs = checkSLURMjobs(jobs, debug = True) # check if all tiles are created and only afterwards go on
        # check if empty files were created
        for row in grid:
            for c in row:
                if str(c) not in excludelist:
                    filepath = os.path.join(outfileroot, getFilePath(c))
                    if os.path.exists(filepath):
                        if isEmpty(filepath): os.remove(filepath) # delete empty output files
    elapsed = time.time() - start
    print('Elapsed time: %g seconds' %(elapsed))





