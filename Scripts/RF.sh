#!/bin/bash
#SBATCH --job-name=RF_pass_1
#SBATCH --output=RF_pass_1.out
#SBATCH --error=RF_pass_1.err
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --partition=sandyb  


module load python/3.5.2+intel-16.0
cd /home/anisfeld/scratch-midway/ml-charters
source /home/anisfeld/scratch-midway/ml-charters/bin/activate

# update config file
touch temp_config.py
cat /home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/config.py >> temp_config.py
echo 'WHICH_GRID = SMALL_GRID' >>  temp_config.py
echo 'TO_RUN = ["RF"]' >> temp_config.py
# cat temp_config.py
echo 'Is this thing on'
python /home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/pipeline_test.py temp_config
