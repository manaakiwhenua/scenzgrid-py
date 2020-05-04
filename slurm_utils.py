#!/usr/bin/env python
"""
Basic tools for submitting jobs such as GDAL calls or python scripts on a SLURM-based
system. Developed on the NeSI Pan Cluster
https://wiki.auckland.ac.nz/display/CERES/NeSI+Pan+Cluster

"""
# This file is part of scenzgrid.py
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
import subprocess
import time
import sys


def submitSLURMjob(commandstring, joblist, debug = False):
    """
    Function that writes a job file for SLURM, submits it and adds its ID to a list
    of jobs.
    """
    template = open('template.sl')
    templatestr = template.read()
    template.close()
    jobstr = templatestr + commandstring
    outfile = open('jobfile.sl', 'w')
    outfile.write(jobstr + '\n')
    outfile.close()
    outputstring = subprocess.check_output(['sbatch', 'jobfile.sl']).decode("utf-8")
    if debug:
        print('outputstring >>>')
        print(outputstring)
        print('<<< outputstring')
    #job = outputstring.split(' ')[3].strip("\"") # we only need the job id
    job = outputstring.split(' ')[3].rstrip()
    joblist.append(job)
    if debug:
        print(job)
    return joblist

def checkSLURMjobs(joblist, timestep = 1, debug = False):
    """
    Checks if all jobs that were so far submitted are finished and afterwards
    empties list of jobs
    """
    if debug:
        print('%d jobs in queue' %(len(joblist)))
        for job in joblist:
            print(job)
    finished = False
    for job in joblist:
        if debug:
            print('current job: %s' %(job))
        #status = os.system('squeue -j %s \| grep %s' %(job, job)).split()[4]
        '''output = subprocess.check_output(['squeue', '-j', job])
        tokens = output.split()
        status = str(tokens[12], encoding='UTF-8')'''
        status = 'PD' # just to get the loop started
        #print('status = >>%s<<' %(status)) 
        while status == 'PD' or status == 'R' or status == 'CG' or status == 'S':            
            if debug:
                print('%s Status: %s; not finished yet' %(job, status))
            #status = os.system('squeue -j %s \| grep %s' %(job, job)).split()[4]
            try: 
                output = subprocess.check_output(['squeue', '-j', job])
                tokens = output.split()
                status = str(tokens[12], encoding='UTF-8')
                #print(status)
            except (subprocess.CalledProcessError, IndexError):
                #print("All current jobs finished")
                #finished = True
                break
            time.sleep(timestep)
        #if finished: break
        print('%s will now be removed from list' %job)
    joblist = []
    return joblist





