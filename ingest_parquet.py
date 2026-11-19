import os

import pandas as pd

HOME = os.environ["HOME"]

# ACT AGN source file from Josh Borrow
sources = pd.read_parquet(f"{HOME}/sources.parquet")

out_file = sources[["name", "ra", "dec"]].copy()
out_file["monitored"] = sources["variable"].astype(bool)
out_file["source_id"] = sources["name"].index.astype(object)

out_file.to_csv(f"{HOME}/sources_for_act.csv", index=False)
