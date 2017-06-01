# Basic overview:

## Write a batch script: 
touch test.sh
nano test.sh  #what goes in the file is detailed below
chmod 700 test.sh

## Run the batch script:
sbatch ./test.sh 




# The Batch Script
A batch script has two components: Header and code

header:

#!/bin/bash
#SBATCH --job-name=test
#SBATCH --output=jobstdout.out
#SBATCH --error=jobstderr.err
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --partition=sandyb


### Notes:
job-name: an arbitrary name that reflects the job
output: a file that recieves all print statements and returns
error: a file that recievse all errors
time: max time allowed (HH:MM:SS)
Nodes: number of computers
n-tasks-per-node: cores used per computer (?) (Victor had 16 by default)
Partition can be: sandyb or mic  


# Code

module load python/3.5.2+intel-16.0
cd /home/anisfeld/scratch-midway/ml-charters
source /home/anisfeld/scratch-midway/ml-charters/bin/activate
python /home/anisfeld/scratch-midway/ml-charters/test.py 


### Notes:
module load makes python available.
You'll need a virtual environment before you run a batch.
I used the code:

cd /home/anisfeld/scratch-midway/
virtualenv ml-charters
cd ml_charters
git clone https://github.com/donuts-and-data-division/charters.git

source /home/anisfeld/scratch-midway/ml-charters/bin/activate
pip install -r requirements.txt


Then run a python file. You can pass an argument if needed.
python /home/anisfeld/scratch-midway/ml-charters/test.py  v1 v2

In the main function of test.py write something like:

	import system
	print(sys.argv[1])  

This will print v1. Note you can't pass multiword strings.

That's it! 

# Useful information:

from an environment already established get requirements
pip freeze > requirements.txt








