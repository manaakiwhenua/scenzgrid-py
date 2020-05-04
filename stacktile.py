#!/usr/bin/env python
"""
Stacks all tiles of one specific rHEALPix cell

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
import numpy
import argparse
import glob

import osgeo.gdal as gdal
from osgeo import ogr
from rios import applier

from data_utils import *

def doStack(info, inputs, outputs):
    """
    Called from RIOS. Stacks the input files
    """
    outputs.outimage = numpy.vstack(inputs.imgs)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("cell", type = str, help="Name of rHEALPix cell.")
    parser.add_argument("indir", type = str, help="Specify the input directory including rHEALPix tiled datasets.")
    parser.add_argument("outdir", type = str, help="Specify the output directory.")
    parser.add_argument("-v", "--verbose", help="Show debug messages (default: False).", action ="store_true")

    args = parser.parse_args()

    if args.cell == None:
        print("Error: no cell specified")
        sys.exit()
    c = args.cell

    if args.indir == None:
        print("Error: no input file specified")
        sys.exit()
    indir = args.indir

    if args.outdir == None:
        print("Error: no output directory specified")
        sys.exit()
    outfileroot = args.outdir

    if args.verbose:
        debug = True
    else:
        debug = False    
   
    layerroots = glob.glob(os.path.join(indir, '*'))
    inputfiles = []
    layernames = []
    for layer in layerroots: # create list of file names for given grid
        if debug: print(c)
        filepath = os.path.join(layer, getFilePath(c))
        if os.path.exists(filepath):
            inputfiles.append(filepath)
            layernames.append(layer.split('/')[-1])
    if not inputfiles == []: #check if any tiles are available for this file
        outputfile = os.path.join(outfileroot, getFilePath(c))
        if debug: print(outputfile)
        if not os.path.exists(os.path.dirname(outputfile)):
            if debug: print('now will create %s' %(os.path.dirname(outputfile)))
            try:
                os.makedirs(os.path.dirname(outputfile))
            except:
                print("Unexpected Error while creating directory")            
        infiles = applier.FilenameAssociations()
        infiles.imgs = inputfiles      
        outfiles = applier.FilenameAssociations()
        outfiles.outimage = outputfile
        controls = applier.ApplierControls()
        controls.setWindowXsize(243)
        controls.setWindowYsize(243)
        controls.setCreationOptions(["IMAGEBLOCKSIZE=243"])
        applier.apply(doStack, infiles, outfiles, controls = controls)
        # now the band names have to be set
        dst_ds = gdal.Open(outputfile, gdal.GA_Update)
        # Check that the image has been opened.
        if not dst_ds is None:
            allbandcount = 0
            for j in range(len(inputfiles)):
                beforebandcount = allbandcount
                src_ds = gdal.Open(inputfiles[j], GA_ReadOnly )
                if not src_ds is None:
                    for currentbandcount in range(1, src_ds.RasterCount+1):
                        band = src_ds.GetRasterBand(currentbandcount)
                        bandname = band.GetDescription()
                        outbandname = layernames[j] + ' ' + bandname                                                 
                        # Get the image band
                        imgBand = dst_ds.GetRasterBand(beforebandcount+currentbandcount)
                        # Check the image band was available.
                        if not imgBand is None:
                            # Set the image band name.
                            imgBand.SetDescription(outbandname)
                        else:
                            print ("Could not open the image band: ", band)                                                 
                        if debug: print('band %d is named %s' %(beforebandcount+currentbandcount, outbandname))
                        allbandcount += 1
                    src_ds = None
                else:
                    print("Could not open the input image file: ", inputfiles[j])
            dst_ds = None
        else:
            print ("Could not open the output image file: ", outputfile) 
