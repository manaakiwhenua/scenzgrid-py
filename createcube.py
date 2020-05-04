#!/usr/bin/env python
"""
'Meta-script' that queries remote sensing database, tiles identified images and
then stacks them together thereby creating a rHEALPix data cube

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
import psycopg2
import argparse
import shutil

import lcrfs

from slurm_utils import *


if __name__ == '__main__':
    start = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("tiledir", type = str, help="Specify the directory where rHEALPix tiled datasets are stored.")
    parser.add_argument("cubedir", type = str, help="Specify the directory where the datacube will be stored.")
    parser.add_argument("scene", help="Specify list of landsat scenes (comma-separated without blanks)")
    parser.add_argument("minres", type = int, help="Specify the minimum output grid resolution.")
    parser.add_argument("maxres", type = int, help="Specify the maximum output grid resolution.")    
    parser.add_argument("-s", "--shapefile", help="Supply shapefile that defines output extent")
    parser.add_argument("-e", "--excludelist", help="Supply file with list of tiles to be excluded (has to match maxres).")
    parser.add_argument("-p", "--parallelism", help="Choice of no (default) or slurm")
    parser.add_argument("-v", "--verbose", help="Show debug messages (default: False).", action ="store_true")

    args = parser.parse_args()

    if args.tiledir == None:
        print("Error: no tile directory specified")
        sys.exit()
    tiledir = args.tiledir    

    if args.cubedir == None:
        print("Error: no datacube directory specified")
        sys.exit()
    cubedir = args.cubedir

    if args.scene == None:
        print("Error: no landsat scene specified")
        sys.exit()
    scene = args.scene.split(',')

    if args.minres == None:
        print("Error: minres not specified")
        sys.exit()
    minresolution = int(args.minres)

    if args.maxres == None:
        print("Error: yres not specified")
        sys.exit()
    maxresolution = int(args.maxres)

    if args.shapefile:
        shapefile = str(args.shapefile)
    else:
        shapefile = None

    if args.excludelist:
        excludelist = args.excludelist
    else:
        excludelist = None

    if args.parallelism == 'slurm':
        parallelism = args.parallelism
    else:
        parallelism = None

    if args.verbose:
        debug = True
    else:
        debug = False
    
    jobs = []  # initialize job list
    # connect to database and select entries based on provided query parameters

    con = psycopg2.connect("dbname='rsdata' user='spatial' host='10.0.111.237' password='tobefilledin'")
    cursor = con.cursor()

    if len(scene) == 1:
        scenestr = "scene = '%s'" %(scene[0])
    else:
        scenestr = "(scene = '%s'" %(scene[0])
        appendstr = ""
        for s in scene[1:]:
            appendstr = appendstr + " OR scene = '%s'" %(s)
        scenestr = scenestr + appendstr + ')'
    print(scenestr)
	    
    #cursor.execute("""SELECT lcrfsfilename FROM landsat_list WHERE product = %s AND sat_type = %s
    #                  and scene = %s  AND cal_date < %s AND cal_date >= %s;""",
    #               ("pa", "landsat8", scene[0], "140501", "140101"))
    #cursor.execute("""SELECT lcrfsfilename FROM landsat_list WHERE product = %s AND scene = %s;""",
    #               ("pa", scene[0]))
    #cursor.execute("""SELECT lcrfsfilename FROM landsat_list WHERE product = ? AND \
    #               (scene = ? OR scene = ?);""",
    #               (product, "p72r88", "p72r89"))

    sql = "SELECT lcrfsfilename FROM landsat_list WHERE product = 'pa' AND %s;" %(scenestr)
    #sql = "SELECT lcrfsfilename FROM landsat_list WHERE product = 'pa' AND %s AND cal_date < '140501' AND cal_date >= '140101';" %(scenestr)
    print(sql)
    cursor.execute(sql)
    
    i = 0
    for row in cursor:
        i += 1
        pa = row[0]
        # create corresponding cloud mask name and ps name
        sensor = lcrfs.lcrfs('sensor', pa)
        ps = lcrfs.change(pa, 'sensor', sensor[:-2] + 'ps')
        flats = lcrfs.change(ps, 'stage', 'flats')
        cloud = lcrfs.change(ps, 'stage', 'cloud')
        # make sure these files actually exist
        flatspath = os.path.join(lcrfs.lcrfs('path', flats), flats)
        cloudpath = os.path.join(lcrfs.lcrfs('path', cloud), cloud)
        if not os.path.exists(flatspath):
            print('%s does not exist' %(flatspath))
            continue
        if not os.path.exists(cloudpath):
            print('%s does not exist' %(cloudpath))
            continue
        flats_fn = os.path.splitext(flats)[0]        
        flatsoutpath = os.path.join(tiledir, flats_fn)
        cloud_fn = os.path.splitext(cloud)[0]
        cloudoutpath = os.path.join(tiledir, cloud_fn)

        # Transform the set of images and tile them using tilerasterlayer.py
        # Existing datasets with same name are overwritten
        optparams = ''
        if shapefile:
            optparams = '--shapefile %s ' %(shapefile)
        if excludelist:
            optparams = optparams + '--excludelist %s ' %(excludelist)
        flatscmd = '''python tilerasterlayer.py %s %s %d %d %s \
        --verbose''' %(flatspath, flatsoutpath, minresolution, maxresolution, optparams)
        print(flatscmd)
        if parallelism == 'slurm': jobs = submitSLURMjob(flatscmd, jobs)
        cloudcmd = '''python tilerasterlayer.py %s %s %d %d %s \
        --resamplingmethod near --verbose''' \
        %(cloudpath, cloudoutpath, minresolution, maxresolution, optparams)
        print(cloudcmd)
        if parallelism == 'slurm': jobs = submitSLURMjob(cloudcmd, jobs)
    con.close()
    if parallelism == 'slurm': jobs = checkSLURMjobs(jobs, debug = True)
    # stack datasets together i.e. create datacube
    stackcmd = '''python stacklayers.py %s %s %d %d %s --parallelism %s \
    --verbose''' %(tiledir, cubedir, minresolution, maxresolution, optparams, parallelism)
    print(stackcmd)
    os.system(stackcmd)
    
    elapsed = time.time() - start
    print('Elapsed time (createcube): %g seconds' %(elapsed))
