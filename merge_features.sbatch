#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 8 # number of cores
#SBATCH --mem 184GB # memory pool for all cores
#SBATCH -t 0-12:00 # time (D-HH:MM)
#SBATCH --array=2000-2016

date #print start time
python merge_features.py --year $SLURM_ARRAY_TASK_ID
date #print end time
