#!/bin/bash
#SBATCH --job-name=best1
#SBATCH --output=best1.out
#SBATCH --error=best1.err
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --partition=sandyb  


module load python/3.5.2+intel-16.0
cd /home/anisfeld/scratch-midway/ml-charters
source /home/anisfeld/scratch-midway/ml-charters/bin/activate
python /home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/pipeline_test.py BEST_GRID RF out_best_1.csv python /home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/pipeline_test.py SMALL_GRID DT out_DT_1.csv ~/home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/queryresults.csv
