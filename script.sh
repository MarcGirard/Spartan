#!/bin/bash
# Created by the University of Melbourne job script generator for SLURM
# Fri Apr 03 2020 20:26:09 GMT+0800 (China Standard Time)

# Partition for the job:
#SBATCH --partition=cloud

# The name of the job:
#SBATCH --job-name="test script"

#SBATCH --nodes=1

# Maximum number of tasks/CPU cores used by the job:
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1

# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
# ends successfully
#SBATCH --mail-type=END

# Use this email address:
#SBATCH --mail-user=jiapengt@student.unimelb.edu.au

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=00:10:00

module load Python/3.6.4-spartan_gcc-6.2.0

# Run the job from the directory where it was launched (default)

# The job command(s):
mpirun -np 8 python test