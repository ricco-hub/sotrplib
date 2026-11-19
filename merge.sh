#!/bin/bash
#SBATCH --job-name=run_pipeline_20170510_20180510_merge
#SBATCH --account=simonsobs
#SBATCH --output=/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/act_lcurves/logs/slurm/output/output_merge_%x_%j.txt
#SBATCH --error=/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/act_lcurves/logs/slurm/error/error_merge_%x_%j.txt
#SBATCH --cpus-per-task=1
#SBATCH --nodes=1                     
#SBATCH --ntasks-per-node=1
#SBATCH --mem=8G
#SBATCH --time=01:00:00
#SBATCH --mail-type=END,FAIL,BEGIN
#SBATCH --mail-user=rvent002@ucr.edu
#SBATCH --qos=test

set -euo pipefail
source "$HOME"/sotrplib/.venv/bin/activate

python3 - <<'EOF'
import glob, pandas as pd

root = "/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/act_lcurves"
files = sorted(glob.glob(f"{root}/shards/shard_*/parquet/one_year/*.parquet"))
print(f"Merging {len(files)} files")
if not files:
    raise SystemExit("No shard parquet files found")

df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)

# df["time"] = pd.to_datetime(df["time"], utc=True)
# df = df.sort_values(["source_id", "frequency", "time"]).reset_index(drop=True)
df.to_parquet(f"{root}/parquet/one_year/merged.parquet")
print(f"Wrote {len(df)} rows")
EOF