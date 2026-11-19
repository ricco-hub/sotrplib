#!/bin/bash
#SBATCH --job-name=run_asteroid_pipeline_20200501_20200701
#SBATCH --account=simonsobs
#SBATCH --output=/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/asteroid_lcurves/logs/slurm/output/output_%x_%j.txt
#SBATCH --error=/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/asteroid_lcurves/logs/slurm/error/error_%x_%j.txt
#SBATCH --nodes=1                     # Number of nodes
#SBATCH --ntasks-per-node=1           # Number of tasks per node
#SBATCH --cpus-per-task=1             # Number of CPU cores per task
#SBATCH --mem=64G
#SBATCH --time=1:00:00                # Maximum runtime (D-HH:MM:SS)
#SBATCH --mail-type=END,FAIL,BEGIN
#SBATCH --mail-user=rvent002@ucr.edu    


bash /home/rv4999/sotrplib/run_asteroid_pipeline.sh