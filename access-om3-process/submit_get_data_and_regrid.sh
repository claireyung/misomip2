#!/bin/bash
#PBS -N misomip_process
#PBS -P g40
#PBS -q normalbw
#PBS -l walltime=12:00:00
#PBS -l mem=250GB
#PBS -l software=netcdf
#PBS -l ncpus=28
#PBS -l storage=gdata/ik11+gdata/xp65+gdata/x77+scratch/x77
#PBS -j oe
#PBS -v no
#PBS -l jobfs=250GB

## Note, run this with:
## qsub -v no=60 submit_get_data_and_regrid.sh

## I/O filenames
# this reads the name of the current run directory to use for output etc:
script_dir=/g/data/x77/cy8964/MISOMIP2/misomip2/access-om3-process
cd $script_dir

# load conda
module use /g/data/xp65/public/modules
module load conda/analysis3-26.08 

# run
python3 get_data_and_regrid.py $no &>> output_get_data_and_regrid_${no}.txt
