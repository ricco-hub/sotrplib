#!/bin/bash
set -euo pipefail
cd /scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/act_lcurves
mkdir -p logs

# Optional one-time setup (ingest, socat-migrate), if you need it:
# setup=$(sbatch --parsable setup.sh)
# dep_setup="--dependency=afterok:${setup}"

# "${dep_setup:-}"
array=$(sbatch --parsable /home/rv4999/sotrplib/run_array.sh)
echo "Array job: $array"

merge=$(sbatch --parsable --dependency=afterok:"${array}" /home/rv4999/sotrplib/merge.sh)
echo "Merge job: $merge (runs after $array succeeds)"