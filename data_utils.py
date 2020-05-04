#!/usr/bin/env python
"""
GDAL,OGR and rHEALPix related custom data utilities

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
# along with this program.  If not, see <http://www.gnu.org/licenses/>

import os
import sys
import time
import argparse

import ogr
from osgeo import gdal, ogr, osr
from gdalconst import *


def getExtent(gt,cols,rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner
    '''
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
            # print(x,y)
        yarr.reverse()
    return ext

def reprojectCoords(coords,src_srs,tgt_srs):
    ''' Reproject a list of x,y coordinates.

        @type geom:     C{tuple/list}
        @param geom:    List of [[x,y],...[x,y]] coordinates
        @type src_srs:  C{osr.SpatialReference}
        @param src_srs: OSR SpatialReference object
        @type tgt_srs:  C{osr.SpatialReference}
        @param tgt_srs: OSR SpatialReference object
        @rtype:         C{tuple/list}
        @return:        List of transformed [[x,y],...[x,y]] coordinates
    '''
    trans_coords=[]
    transform = osr.CoordinateTransformation( src_srs, tgt_srs)
    for x,y in coords:
        x,y,z = transform.TransformPoint(x,y)
        trans_coords.append([x,y])
    return trans_coords


def getFilePath(cell):
    ''' Create a scenzgrid file path out of the name of a single
    rHEALPix cell '''
    cellstr = str(cell)
    tmpstr = cellstr
    face = cellstr[0]
    tmpstr = cellstr[1:]
    filepath = face
    level = len(cellstr)-2
    noiter = level // 3
    for i in range(noiter):  
        filepath = os.path.join(filepath, tmpstr[0:3])
        tmpstr = tmpstr[3 :]
    if len(cellstr) == 1:
        filepath = face + ".kea"
    else:
        filepath = os.path.join(filepath, tmpstr + ".kea")
    return filepath


def isEmpty(filename):
    ''' Returns True if a raster tile does not contain any valid data
    As side effect, statistics are calculated for the file '''
    src_ds = gdal.Open(filename, GA_Update )
    empty = True
    for band in range(1, src_ds.RasterCount+1):
        stats =   src_ds.GetRasterBand(1).GetStatistics(0,1)
        # print(stats[2], '\n')
        if str(stats[2]) == 'nan':
            print('Empty band detected.')
        else:
            empty = False
    if empty: print('Empty tile detected.')
    src_ds = None

    return empty



