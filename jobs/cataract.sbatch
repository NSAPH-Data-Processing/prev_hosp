#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 8 # number of cores
#SBATCH --mem 60GB # memory pool for all cores
#SBATCH -t 0-02:00 # time (D-HH:MM)
#SBATCH --array=2000-2016


python generate_features.py --year $SLURM_ARRAY_TASK_ID --condition "cataract"