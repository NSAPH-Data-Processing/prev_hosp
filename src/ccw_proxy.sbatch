#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 8 # number of cores
#SBATCH --mem 60GB # memory pool for all cores
#SBATCH -t 0-02:00 # time (D-HH:MM)
#SBATCH --array=2000-2016

module load Anaconda3/2022.05
source activate medicare_QC
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "ami" --claims_criteria "first_two"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "alzh"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "alzhdmta"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "atrialfb" --claims_criteria "first_two"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "cataract" --claims_criteria "primary"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "chrnkidn"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "copd"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "chf"
python ccw_proxy.py --year $SLURM_ARRAY_TASK_ID --condition "diabetes"
