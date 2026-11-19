#!/bin/bash
#SBATCH --job-name=run_pipeline_20170510_20180510_array
#SBATCH --account=simonsobs
#SBATCH --output=/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/act_lcurves/logs/slurm/output/output_array_%x_%j.txt
#SBATCH --error=/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/act_lcurves/logs/slurm/error/error_array_%x_%j.txt
#SBATCH --array=0-19
#SBATCH --cpus-per-task=1
#SBATCH --nodes=1                     
#SBATCH --ntasks-per-node=1
#SBATCH --mem=32G
#SBATCH --time=05:00:00
#SBATCH --mail-type=END,FAIL,BEGIN
#SBATCH --mail-user=rvent002@ucr.edu
#SBATCH --qos=short

set -euo pipefail
source $HOME/sotrplib/.venv/bin/activate

WORKDIR="/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/act_lcurves"
SHARD=${SLURM_ARRAY_TASK_ID}
NSHARDS=${SLURM_ARRAY_TASK_COUNT}
SHARD_DIR="${WORKDIR}/shards/shard_$(printf '%02d' "$SHARD")"
mkdir -p "$SHARD_DIR"/{pngs,results,parquet/one_year}
cd "$SHARD_DIR"

# Same env as before
export DEPTH_ONE_PARENT=/scratch/gpfs/SIMONSOBS/so/maps/actpol/depth1/
export mapcat_depth_one_parent=$DEPTH_ONE_PARENT
export socat_client_client_type=db
export socat_model_database_name=$HOME/socat/socat_parquet.db
export mapcat_database_type=sqlite

# Per-shard mapcat copy to avoid SQLite write locks (see note below)
export mapcat_database_name="$SHARD_DIR/mapcat.sqlite"
cp "$HOME/testing_pipeline/mapcat.sqlite" "$mapcat_database_name"

# Generate this shard's config
SHARD=$SHARD NSHARDS=$NSHARDS SHARD_DIR=$SHARD_DIR python3 - <<'EOF'
import json, os
from datetime import datetime, timedelta, timezone

cfg = json.load(open(os.path.expandvars("$HOME/sotrplib/full_pipeline.json")))
shard, n, d = int(os.environ["SHARD"]), int(os.environ["NSHARDS"]), os.environ["SHARD_DIR"]

fmt = "%Y-%m-%dT%H:%M:%SZ"
t0 = datetime.strptime(cfg["maps"]["start_time"], fmt).replace(tzinfo=timezone.utc)
t1 = datetime.strptime(cfg["maps"]["end_time"], fmt).replace(tzinfo=timezone.utc)
step = (t1 - t0) / n
cfg["maps"]["start_time"] = (t0 + step * shard).strftime(fmt)
cfg["maps"]["end_time"]   = (t0 + step * (shard + 1)).strftime(fmt)

# Redirect outputs into the shard dir
for out in cfg["source_outputs"]:
    sub = {"cutout": "pngs", "pickle": "results", "parquet": "parquet/one_year"}[out["output_type"]]
    out["directory"] = f"{d}/{sub}"

json.dump(cfg, open("config.json", "w"), indent=2)
print(cfg["maps"]["start_time"], "->", cfg["maps"]["end_time"])
EOF

sotrp -c config.json