#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 8 # number of cores
#SBATCH --mem 184GB # memory pool for all cores
#SBATCH -t 0-12:00 # time (D-HH:MM)

date #print start time
snakemake --cores 6
date #print end time