#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 12 # number of cores
#SBATCH --mem 120GB # memory pool for all cores
#SBATCH -t 0-04:00 # time (D-HH:MM)
#SBATCH --array=2000-2016

module load python/3.10.9-fasrc01
mamba activate prev_hosp_env
python output.py --year $SLURM_ARRAY_TASK_ID
