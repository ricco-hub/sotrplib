#!/bin/bash
set -euo pipefail

source $HOME/sotrplib/.venv/bin/activate

# Logging output from running sotrplib command
LOG_DIR=$HOME/testing_pipeline/logs
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Logging to $LOG_FILE"

# set depth-1 dir
export DEPTH_ONE_PARENT=$HOME/testing_pipeline/

# set variables for socat and mapcat
export socat_client_client_type=db
export socat_model_database_name=$HOME/socat/socat_parquet.db
export mapcat_database_name=$HOME/mapcat/mapcat.db

# One-time setup: INGEST_SOURCES=1 bash run_pipeline.sh
if [ "${INGEST_SOURCES:-0}" = "1" ]; then
    if [ -f "$socat_model_database_name" ]; then
        echo "ERROR: $socat_model_database_name already exists. Delete it first to re-ingest." >&2
        exit 1
    fi
# HACK 
    python -c "
import pandas as pd
database = pd.read_parquet('/home/rv4999/sources.parquet')
out_file = database[['name','ra','dec']].copy()
out_file['monitored'] = database['variable'].astype(bool)
out_file.to_csv('/home/rv4999/sources_for_act.csv', index=False)
"
    socat-migrate
    socat-text -f /home/rv4999/sources_for_act.csv
fi

# Check if the DB exists (so it can't create an empty one)
if [ -f "$socat_model_database_name" ]; then
python -W ignore - <<'EOF'
from socat.client.settings import SOCatClientSettings
from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy import units as u
c = SOCatClientSettings().client
res = c.get_box(lower_left=SkyCoord(ra=0*u.deg, dec=-90*u.deg),
                upper_right=SkyCoord(ra=359.9*u.deg, dec=90*u.deg),
                t_min=Time("2017-05-10T00:00:00Z"), t_max=Time("2017-05-12T00:00:00Z"))
print(f"socat sources in DB: {len(res)}")
EOF
fi

# make output dirs as required by the JSON file
mkdir -p "$HOME"/testing_pipeline/pngs "$HOME"/testing_pipeline/results

# only for a new batch of maps:
# actingest -r "$DEPTH_ONE_PARENT" -g "*/*_map.fits" -t act

# run the full pipeline
sotrp -c full_pipeline.json

echo "Finished. Log: $LOG_FILE"

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