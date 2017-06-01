#!/bin/bash
#SBATCH --job-name=RF
#SBATCH --output=RF1.out
#SBATCH --error=RF2.err
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --partition=sandyb  


# update config file
touch temp_config.py
cat /home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/config.py >> temp_config.py
echo 'WHICH_GRID = SMALL_GRID' >>  temp_config.py
echo 'TO_RUN = ["RF"]' >> temp_config.py
# cat temp_config.py
echo 'Is this thing on'

module load python/3.5.2+intel-16.0
cd /home/anisfeld/scratch-midway/ml-charters
source /home/anisfeld/scratch-midway/ml-charters/bin/activate
python /home/anisfeld/scratch-midway/ml-charters/charters/Pipeline/pipeline_test.py temp_config
python /home/anisfeld/scratch-midway/ml-charters/test.py FUCK THE HELLO
