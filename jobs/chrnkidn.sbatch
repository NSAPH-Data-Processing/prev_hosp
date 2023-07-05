#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 8 # number of cores
#SBATCH --mem 60GB # memory pool for all cores
#SBATCH -t 0-02:00 # time (D-HH:MM)
#SBATCH --array=2000-2016

module load python/3.10.9-fasrc01
mamba activate prev_hosp_env
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "chrnkidn"