#!/bin/bash
set -euo pipefail

source $HOME/sotrplib/.venv/bin/activate

BASE_DIR="/scratch/gpfs/SIMONSOBS/users/rv4999"
WORKDIR="${BASE_DIR}/testing_pipeline/asteroid_lcurves/ceres_so"
mkdir -p "$WORKDIR"
cd "$WORKDIR" || exit

if [ -e "$HOME"/testing_pipeline/asteroid_lcurves/data ]; then
  echo "$HOME/testing_pipeline/asteroid_lcurves/data exists. You should remove it before re-running."
  exit 1
fi

# Logging output from running sotrplib command
LOG_DIR=${WORKDIR}/logs/run_logs
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Logging to $LOG_FILE"

# depth-1 dir
export DEPTH_ONE_PARENT=/scratch/gpfs/SIMONSOBS/lat-iso/phase2/depth1/out_wide/ #/scratch/gpfs/SIMONSOBS/so/maps/actpol/depth1/ #$HOME/testing_pipeline/

# set up socat and mapcat
export socat_client_client_type=db
export socat_model_database_name="${WORKDIR}/socat_asteroid_for_usage.db"
export mapcat_depth_one_parent=/scratch/gpfs/SIMONSOBS/lat-iso/phase2/depth1/out_wide/ #/scratch/gpfs/SIMONSOBS/so/maps/actpol/depth1/ #$HOME/testing_pipeline/
# export mapcat_database_name=$HOME/testing_pipeline/mapcat.sqlite #$HOME/testing_pipeline/mapcat_14944.sqlite
# rm -f "$mapcat_database_name"
# # sqlite3 "$HOME/testing_pipeline/mapcat.sqlite" .schema | sqlite3 "$mapcat_database_name"
# python3 - <<'EOF'
# import os, sqlite3
# src = sqlite3.connect(os.path.expandvars("$HOME/testing_pipeline/mapcat.sqlite"))
# dst = sqlite3.connect(os.environ["mapcat_database_name"])
# for (sql,) in src.execute(
#     "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'"
# ):
#     dst.execute(sql)
# dst.commit()
# EOF
# # export mapcat_database_name=$HOME/testing_pipeline/mapcat.sqlite #$HOME/mapcat/mapcat.db
# export mapcat_database_type=sqlite

export mapcat_database_name="$HOME/mapcat/lat_iso_phase2_out_wide.sqlite" #"$HOME/testing_pipeline/mapcat.sqlite" #"${WORKDIR}/mapcat_${SLURM_JOB_ID:-local}.sqlite"   # working copy, recreated each run
# rm -f "$mapcat_database_name"

# NOTE: this specific step is to copy over only relevant depth-1 maps from the original mapcat database
# python3 - <<'EOF'
# import os, sqlite3
# src = sqlite3.connect(f"file:{os.environ['MAPCAT_TEMPLATE']}?mode=ro", uri=True)
# dst = sqlite3.connect(os.environ["mapcat_database_name"])
# for (sql,) in src.execute(
#     "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'"
# ):
#     dst.execute(sql)
# dst.commit()
# EOF
export mapcat_database_type=sqlite

# actingest -r "$mapcat_depth_one_parent" -g "*/*_map.fits" -t act

# One-time setup for Ceres SOCAT ingestion
if [ "${INGEST_SOURCES:-0}" = "1" ]; then
    mkdir -p "$(dirname "$socat_model_database_name")"

    socat-migrate

    CERES_COUNT=$(sqlite3 "$socat_model_database_name" \
        "SELECT COUNT(*) FROM solarsystem_objects WHERE MPC_id = 1;")

    if [ "$CERES_COUNT" -eq 0 ]; then
        socat-mpc-parquet \
            --db \
            -f "/home/rv4999/asteroids/asteroid_ephemeris_for_estimating_compute_usage.parquet"
    else
        echo "Ceres already exists in SOCAT database. Skipping ingestion."
    fi
fi

# python3 "$HOME"/sotrplib/scripts/end_to_end/lightserver.py &

# sleep 3

# curl -X PUT http://localhost:8001/instruments/ \
#   -H "Content-Type: application/json" \
#   -d '{
#         "telescope": "act",
#         "instrument": "ACT",
#         "frequency": 90,
#         "module": "pa5",
#         "details": {}
#       }'


# lightcurvedb-socat

# make output dirs for the JSON config file
mkdir -p "${WORKDIR}"/pngs "${WORKDIR}"/results "${WORKDIR}/parquet"

sotrp -c "$HOME"/sotrplib/full_pipeline.json

echo "Finished. Log at: $LOG_FILE"

        # {
        #     "output_type": "lightserve",
        #     "hostname": "http://localhost:8001/"
        # },
# NOTE: code below works (pre-PR #190)
# source $HOME/sotrplib/.venv/bin/activate

# # Logging output from running sotrplib command
# LOG_DIR=$HOME/testing_pipeline/logs
# mkdir -p "$LOG_DIR"
# LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S).log"
# exec > >(tee -a "$LOG_FILE") 2>&1
# echo "Logging to $LOG_FILE"

# # set depth-1 dir
# export DEPTH_ONE_PARENT=$HOME/testing_pipeline/

# # set variables for socat and mapcat
# export socat_client_client_type=db
# export socat_model_database_name=$HOME/socat/socat_parquet.db
# export mapcat_database_name=$HOME/mapcat/mapcat.db

# # One-time setup: INGEST_SOURCES=1 bash run_pipeline.sh
# if [ "${INGEST_SOURCES:-0}" = "1" ]; then
#     if [ -f "$socat_model_database_name" ]; then
#         echo "ERROR: $socat_model_database_name already exists. Delete it first to re-ingest." >&2
#         exit 1
#     fi
# # HACK 
#     python -c "
# import pandas as pd
# database = pd.read_parquet('/home/rv4999/sources.parquet')
# out_file = database[['name','ra','dec']].copy()
# out_file['monitored'] = database['variable'].astype(bool)
# out_file.to_csv('/home/rv4999/sources_for_act.csv', index=False)
# "
#     socat-migrate
#     socat-text -f /home/rv4999/sources_for_act.csv
# fi

# # Check if the DB exists (so it can't create an empty one)
# if [ -f "$socat_model_database_name" ]; then
# python -W ignore - <<'EOF'
# from socat.client.settings import SOCatClientSettings
# from astropy.coordinates import SkyCoord
# from astropy.time import Time
# from astropy import units as u
# c = SOCatClientSettings().client
# res = c.get_box(lower_left=SkyCoord(ra=0*u.deg, dec=-90*u.deg),
#                 upper_right=SkyCoord(ra=359.9*u.deg, dec=90*u.deg),
#                 t_min=Time("2017-05-10T00:00:00Z"), t_max=Time("2017-05-12T00:00:00Z"))
# print(f"socat sources in DB: {len(res)}")
# EOF
# fi

# # make output dirs as required by the JSON file
# mkdir -p "$HOME"/testing_pipeline/pngs "$HOME"/testing_pipeline/results

# # only for a new batch of maps:
# # actingest -r "$DEPTH_ONE_PARENT" -g "*/*_map.fits" -t act

# # run the full pipeline
# sotrp -c full_pipeline.json

# echo "Finished. Log: $LOG_FILE"

# # all ACT depth-1 maps: /scratch/gpfs/SIMONSOBS/so/maps/actpol/depth1/
# export DEPTH_ONE_PARENT=/home/rv4999/testing_pipeline/

# # setting up socat by pointing to source catalog database
# export socat_client_client_type=db
# export socat_model_database_name=/home/rv4999/socat/socat_test.db

# # setting up mapcat by pointing to map catalog database
# export mapcat_database_name=/home/rv4999/mapcat/mapcat.db
# # only call for new batch of maps:
# # actingest -r /home/rv4999/testing_pipeline/ -g "*/*_map.fits" -t act # should this be replaced by DEPTH_ONE_PARENT?

# sotrp -c sample_read_act_mapcat.json