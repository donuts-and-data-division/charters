#!/bin/bash
#SBATCH --job-name=RF
#SBATCH --output=RF1.out
#SBATCH --error=RF1.err
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --partition=sandyb  


module load python/3.5.2+intel-16.0
cd /home/anisfeld/scratch-midway/ml-charters
source /home/anisfeld/scratch-midway/ml-charters/bin/activate
python /home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/pipeline_test.py SMALL_GRID RF out_rf_1.csv
