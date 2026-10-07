from __future__ import annotations

import pandas as pd
import xarray as xr

from src.helper_utils import load_config


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lowered = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


# VERA4cast-style long-format files stack multiple variables under one observation
# column; these are the accepted spellings for the chlorophyll-a variable.
_CHLA_VARIABLE_NAMES = {"chla", "chla_ugl_mean", "chlorophyll_a", "chlorophyll-a"}


def pull_target_data(start_time, end_time) -> xr.Dataset:
    """Pull RC4CAST/VERA4cast target chlorophyll observations and return xarray dataset with chla."""
    config = load_config("model_config.yml")
    url = config["target_url"]
    df = pd.read_csv(url, compression="infer")

    time_col = _pick_column(df, ["time", "datetime", "date"])
    site_col = _pick_column(df, ["site_id", "site", "location_id", "station_id"])
    value_col = _pick_column(df, ["chla", "observation", "value"])
    variable_col = _pick_column(df, ["variable"])

    if time_col is None or site_col is None or value_col is None:
        raise ValueError(
            "Could not identify time/site/value columns in target data. "
            f"Found columns: {list(df.columns)}"
        )

    # Long-format files (e.g. VERA4cast daily-insitu-targets) mix many variables
    # together; keep only chlorophyll-a rows before pivoting.
    if variable_col is not None:
        mask = df[variable_col].astype(str).str.strip().str.lower().isin(_CHLA_VARIABLE_NAMES)
        if not mask.any():
            raise ValueError(
                f"No rows matched known chlorophyll-a variable names in column '{variable_col}'. "
                f"Found values: {sorted(df[variable_col].dropna().unique())[:20]}"
            )
        df = df[mask]

    out = df[[time_col, site_col, value_col]].copy()
    out.columns = ["time", "site_id", "chla"]
    out["time"] = pd.to_datetime(out["time"], errors="coerce").dt.tz_localize(None)
    out["site_id"] = out["site_id"].astype(str).str.strip()
    numeric_mask = out["site_id"].str.match(r"^\d+$")
    out.loc[numeric_mask, "site_id"] = "USGS-" + out.loc[numeric_mask, "site_id"]
    out["chla"] = pd.to_numeric(out["chla"], errors="coerce")

    out = out.dropna(subset=["time", "site_id"]) 
    out = out[(out["time"] >= pd.to_datetime(start_time)) & (out["time"] <= pd.to_datetime(end_time))]

    pivot = out.pivot_table(index="time", columns="site_id", values="chla", aggfunc="mean")
    pivot = pivot.sort_index().sort_index(axis=1)

    return xr.Dataset(
        data_vars={"chla": (("time", "site_id"), pivot.to_numpy())},
        coords={"time": pivot.index.to_numpy(), "site_id": pivot.columns.to_numpy()},
    )
