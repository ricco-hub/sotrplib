import argparse as ap
import os
import sqlite3
from datetime import datetime, timezone
from getpass import getuser
from glob import glob
from pathlib import Path

from tqdm import tqdm

USER = getuser()

P = ap.ArgumentParser(
    description="Run sotrp on depth1 map in slurm job. Search maps in directory and make a .json config file for each observation.",
    formatter_class=ap.ArgumentDefaultsHelpFormatter,
)

P.add_argument(
    "--maps",
    action="store",
    default=[],
    nargs="+",
    help="Depth1 maps to analyze. Will null anything in date-dirs and only analyze these maps.",
)

P.add_argument(
    "--date-dirs",
    action="store",
    default=[],
    nargs="+",
    help="5 digit date directories to glob for maps to analyze. can use wildcard; i.e. 15* will glob all date dirs starting with 15",
)

P.add_argument(
    "-s",
    "--snr-threshold",
    help="SNR threshold for source finding",
    default=5.0,
    type=float,
)

P.add_argument(
    "--data-dir",
    action="store",
    default="/scratch/gpfs/SIMONSOBS/so/maps/actpol/depth1/",
    help="Data directory, where the depth1 maps live.",
)

P.add_argument(
    "--mapcat-db",
    default=str(Path.home() / "testing_pipeline/mapcat.sqlite"),
    help="Existing MapCat SQLite database containing the depth-1 maps.",
)

P.add_argument(
    "--sim-transients",
    help="Inject transients into the maps.",
    action="store_true",
)

P.add_argument(
    "--pointing-flux-threshold",
    action="store",
    default="0.3 Jy",
    type=str,
    help="Flux threshold for pointing sources, json compatible quantity.",
)

P.add_argument(
    "--flux-threshold",
    action="store",
    default="10 mJy",
    type=str,
    help="Flux threshold for source extraction, json compatible quantity.",
)

P.add_argument(
    "--scratch-dir",
    action="store",
    default=f"/scratch/gpfs/SIMONSOBS/users/{USER}/scratch/",
    help="Location of scratch directory for users. Default: /scratch/gpfs/SIMONSOBS/users/[user]/scratch/.",
)

P.add_argument(
    "--script-dir",
    action="store",
    default="",
    help="Directory in which the trp_pipeline script lives. If default, then will use os.cwd() which assumes this script lives in the same directory.",
)

P.add_argument(
    "--out-dir",
    action="store",
    default="",
    help="Output directory for output files. If empty, will create directory in scratch_dir.",
)

P.add_argument(
    "--group-name",
    action="store",
    default="simonsobs",
    help="Name of computing cluster group that you belong to, for running slurm jobs.",
)

P.add_argument(
    "--slurm-script-dir",
    action="store",
    default="slurm_job_scripts/",
    help="Directory in which the slurm job scripts lives.",
)

P.add_argument(
    "--slurm-out-dir",
    action="store",
    default="slurm_output_files/",
    help="Directory in which the slurm output files get saved. If default, appends to scratch dir. ",
)

P.add_argument(
    "--save-thumbnails",
    action="store_true",
    default=True,
    help="Save thumbnail maps around the transient sources. ",
)

P.add_argument(
    "--thumbnail-radius",
    action="store",
    default="0.1 deg",
    type=str,
    help="Thumbnail radius (half-width of square map), json compatible quantity.",
)

P.add_argument(
    "--ncores",
    action="store",
    default=12,
    type=int,
    help="Number of cores to request for each slurm job. ",
)

P.add_argument(
    "--nserial",
    action="store",
    default=8,
    type=int,
    help="Number of serial sets of `ncores` scripts to run per job. ",
)

P.add_argument(
    "--bands",
    action="store",
    nargs="+",
    default=["f090", "f150", "f220"],
    help="Bands to analyze, default is f090,f150,f220,f280.",
)

args = P.parse_args()


def generate_slurm_header(jobname, groupname, cpu_per_task, script_dir, slurm_out_dir):
    slurm_header = f"""#!/bin/bash
#SBATCH --job-name={jobname}            # create a short name for your job
#SBATCH -A {groupname}                    # group name, by default simonsobs
#SBATCH --nodes=1                # node count
#SBATCH --ntasks=1               # total number of tasks across all nodes
#SBATCH --cpus-per-task={cpu_per_task}       # cpu-cores per task (>1 if multi-threaded tasks)
#SBATCH --mem-per-cpu=64G         # memory per cpu-core (4G is default)
#SBATCH --time=04:59:00          # total run time limit (HH:MM:SS)
#SBATCH --output={slurm_out_dir}%x_%j.out
#SBATCH --error={slurm_out_dir}%x_%j.err
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=rvent002@ucr.edu

module load soconda/3.12/v0.6.18

export SRUN_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK
cd {script_dir}

source /home/rv4999/sotrplib/.venv/bin/activate

export socat_client_client_type=db
export socat_model_database_name=/scratch/gpfs/SIMONSOBS/users/rv4999/testing_pipeline/asteroid_lcurves/socat_asteroid_for_usage.db # export socat_model_database_name=$HOME/socat/socat_parquet.db
export mapcat_depth_one_parent=/scratch/gpfs/SIMONSOBS/lat-iso/phase2/depth1/out_wide/ # /scratch/gpfs/SIMONSOBS/so/maps/actpol/depth1/
export mapcat_database_name="$HOME/mapcat/lat_iso_phase2_out_wide.sqlite" # "$HOME/testing_pipeline/mapcat.sqlite"
export mapcat_database_type=sqlite

mkdir -p "$(dirname "$socat_model_database_name")"

# mkdir -p "{slurm_out_dir}png" "{slurm_out_dir}pickle" "{slurm_out_dir}parquet"
mkdir -p "{args.out_dir}/png" "{args.out_dir}/pickle" "{args.out_dir}/parquet"

"""
    return slurm_header


START_TIME = datetime.fromisoformat("2017-01-01T00:00:00+00:00")
END_TIME = datetime.fromisoformat("2023-01-01T00:00:00+00:00")


def get_map_id(mapfile, data_dir, db_path):
    path = Path(mapfile).resolve()
    parent = Path(data_dir).resolve()

    relative_path = path.relative_to(parent).as_posix()

    if relative_path.endswith("_rho.fits"):
        relative_path = relative_path[: -len("_rho.fits")] + "_map.fits"

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT map_id, start_time
            FROM depth_one_maps
            WHERE map_path = ?
            """,
            (relative_path,),
        ).fetchone()

    if row is None:
        raise ValueError(f"Map not found in MapCat: {relative_path}")

    map_id, start_time_str = row

    map_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))

    if map_time.tzinfo is None:
        map_time = map_time.replace(tzinfo=timezone.utc)

    if not (START_TIME <= map_time < END_TIME):
        return None

    return map_id


def generate_config_json(
    mapfile,
    mad_id,
    pointing_flux_threshold="0.3 Jy",
    flux_low_limit="0.01 Jy",
    thumbnail_half_width="0.2 deg",
    min_snr=5.0,
    min_pointing_sources=10,
    output_dir="./",
):
    # kappa_map_file = mapfile.replace("_rho.fits", "_kappa.fits")
    # time_map_file = mapfile.replace("_rho.fits", "_time.fits")
    # unix_time = int(mapfile.split("/")[-1].split("_")[1])
    # start_time = Time(unix_time - 3 * 86400, format="unix")
    # stop_time = Time(unix_time + 3 * 86400, format="unix")
    # info_file = mapfile.replace("_rho.fits", "_info.hdf")
    freq = mapfile.split("_")[-2]
    arr = mapfile.split("_")[-3]
    config_text = f"""{{
    "maps":
        {{
            "map_generator_type": "mapcat_database",
            "instrument": "ACT",
            "map_type": "rhokappa",
            "map_units": "mJy",
            "map_ids": ["{mad_id}"],
            "start_time": "2017-01-01T00:00:00Z",
            "end_time": "2023-01-01T00:00:00Z",
            "frequency": "{freq}",
            "array": "{arr}",
            "rerun": "True"
        }},
    "source_catalogs": [
        {{
            "catalog_type": "socat"
        }}
    ],
    "preprocessors": [
        {{
            "preprocessor_type": "planet_mask",
            "mask_radius": "15 arcmin"
        }},
        {{
            "preprocessor_type": "kappa_rho"
        }},
        {{
            "preprocessor_type": "edge_mask",
            "mask_on": "kappa",
            "edge_width": "10.0 arcmin"
        }}
    ],
    "postprocessors": [
       {{
            "postprocessor_type": "flatfield",
            "sigma_val": 5.0,
            "tile_size": "0.5 deg"
        }}
    ],
    "source_subtractor": {{
            "subtractor_type": "photutils"
    }},
    "forced_photometry": {{
        "photometry_type": "lmfit",
        "reproject_thumbnails": "True",
        "flux_limit_centroid": "0.1 Jy",
        "thumbnail_half_width": "4 arcmin",
        "allowable_center_offset": "1.0 arcmin",
        "near_source_rel_flux_limit": 1.0
    }},
    "sifter": {{
        "sifter_type": "default"
    }},
    "source_outputs": [
        {{
            "output_type": "cutout",
            "directory": "{output_dir}png"
        }},
        {{
            "output_type": "pickle",
            "directory": "{output_dir}pickle"
        }},
        {{
            "output_type": "parquet", 
            "output_source_candidates": false,
            "output_transient_candidates": false, 
            "directory": "{output_dir}parquet"
        }}
    ]
}}
"""
    return config_text


nserial = args.ncores * args.nserial

datelist = []
if not args.maps:
    if len(args.date_dirs) == 0:
        datelist = sorted(glob(args.data_dir + "*"))
    elif len(args.date_dirs) == 1:
        datelist = sorted(glob(args.data_dir + args.date_dirs[0]))
    else:
        for o in args.date_dirs:
            datelist += sorted(glob(args.data_dir + o))

print("There are ", len(datelist), " date directories total")

## make scratch output directory
user_scratch = args.scratch_dir
if not os.path.exists(user_scratch):
    os.mkdir(user_scratch)

if not args.out_dir:
    ## get random tmp dir
    import random
    import string

    letters = string.ascii_lowercase
    random_string = "".join(random.choice(letters) for i in range(6))

    args.out_dir = user_scratch + f"tmp_sotrpdir_{random_string}/"
    print("Creating temp output directory: ", args.out_dir)

## set slurm output dir to live in the temp directory
if args.slurm_out_dir == P.get_default("slurm_out_dir"):
    args.slurm_out_dir = args.out_dir + args.slurm_out_dir
if args.slurm_script_dir == P.get_default("slurm_script_dir"):
    args.slurm_script_dir = args.out_dir + args.slurm_script_dir

# if not os.path.exists(args.out_dir):
#     os.mkdir(args.out_dir)
# if not os.path.exists(args.slurm_script_dir):
#     os.mkdir(args.slurm_script_dir)
for directory in (
    args.out_dir,
    args.slurm_script_dir,
    args.slurm_out_dir,
    os.path.join(args.out_dir, "png"),
    os.path.join(args.out_dir, "pickle"),
    os.path.join(args.out_dir, "parquet"),
):
    os.makedirs(directory, exist_ok=True)

n = 0
slurm_text = generate_slurm_header(
    str(n).zfill(4),
    args.group_name,
    str(args.ncores),
    args.script_dir if args.script_dir else os.getcwd(),
    args.slurm_out_dir,
)

scheduled_map_ids = set()
nmaps = 0
for mapfile in args.maps:
    map_id = get_map_id(mapfile, args.data_dir, args.mapcat_db)
    obsid = mapfile.split("/")[-1].split("_")[1]
    dateid = mapfile.split("/")[-2]
    mapname = mapfile.split("/")[-1].replace(".fits", "")
    config_file = args.slurm_script_dir + f"{mapname}_config.json"

    if map_id is None:
        continue

    if map_id in scheduled_map_ids:
        print(f"Skipping duplicate: {mapfile} ({map_id})")
        continue

    scheduled_map_ids.add(map_id)

    with open(config_file, "w") as f:
        f.write(
            generate_config_json(
                mapfile,
                map_id,
                pointing_flux_threshold=args.pointing_flux_threshold,
                flux_low_limit=args.flux_threshold,
                thumbnail_half_width=args.thumbnail_radius,
                min_snr=args.snr_threshold,
                output_dir=args.out_dir,
            )
        )
    slurm_text += f"srun --overlap sotrp -c {config_file}  &\n sleep 1 \n"
    nmaps += 1
    if nmaps % args.ncores == 0 and nmaps > 0:
        slurm_text += "wait\n"

    if nmaps % nserial == 0 and nmaps > 0:
        with open(
            "%s/%s_sub.slurm" % (args.slurm_script_dir, str(n).zfill(4)), "w"
        ) as f:
            f.write(slurm_text)
            f.write("wait")
        n += 1
        slurm_text = generate_slurm_header(
            str(n).zfill(4),
            args.group_name,
            str(args.ncores),
            args.script_dir if args.script_dir else os.getcwd(),
            args.slurm_out_dir,
        )

for i in tqdm(range(len(datelist))):
    date = datelist[i].split("/")[-1]
    for band in args.bands:
        globstr = args.data_dir + date + f"/depth1*{band}*_rho.fits"
        mapfiles = sorted(glob(globstr))
        for mapfile in sorted(mapfiles):
            map_id = get_map_id(mapfile, args.data_dir, args.mapcat_db)
            if map_id is None:
                continue

            if map_id in scheduled_map_ids:
                print(f"Skipping duplicate: {mapfile} ({map_id})")
                continue

            scheduled_map_ids.add(map_id)

            mapname = mapfile.split("/")[-1].replace(".fits", "")
            config_file = args.slurm_script_dir + f"{mapname}_config.json"
            with open(config_file, "w") as f:
                f.write(
                    generate_config_json(
                        mapfile,
                        map_id,
                        pointing_flux_threshold=args.pointing_flux_threshold,
                        flux_low_limit=args.flux_threshold,
                        thumbnail_half_width=args.thumbnail_radius,
                        min_snr=args.snr_threshold,
                        output_dir=args.out_dir,
                    )
                )
            slurm_text += (
                f"srun --overlap sotrp -c {config_file} > {args.out_dir}{mapname}_sotrp.log "
                f" &\n sleep 1 \n"
            )
            nmaps += 1
            if nmaps % args.ncores == 0 and nmaps > 0:
                slurm_text += "wait\n"
            if nmaps % nserial == 0 and nmaps > 0:
                with open(
                    "%s/%s_sub.slurm" % (args.slurm_script_dir, str(n).zfill(4)), "w"
                ) as f:
                    f.write(slurm_text)
                    f.write("wait")
                n += 1
                slurm_text = generate_slurm_header(
                    str(n).zfill(4),
                    args.group_name,
                    str(args.ncores),
                    args.script_dir if args.script_dir else os.getcwd(),
                    args.slurm_out_dir,
                )


with open("%s/%s_sub.slurm" % (args.slurm_script_dir, str(n).zfill(4)), "w") as f:
    f.write(slurm_text)
    f.write("wait")

print("#" * 50)
print("Slurm scripts saved to :", args.slurm_script_dir)
print("Slurm output saved to :", args.slurm_out_dir)

print("Output files saved to :", args.out_dir)

print(
    "To run the slurm jobs, point slurm_submitter.py to the slurm script location stated above."
)

print("#" * 50)
