from __future__ import annotations

import pandas as pd


def pull_vera4cast_inflow(url: str, site_map: dict, start_time, end_time) -> pd.DataFrame:
    """Pull VERA4cast inflow stream discharge (Flow_cms_mean) remapped to reservoir site_ids.

    `site_map` maps an inflow stream's site_id (e.g. "tubr") to the reservoir
    site_id that should receive its discharge as `river_discharge` (e.g. "fcre"),
    since reservoirs don't have their own discharge gauge.

    Returns a DataFrame with columns [time, site_id, river_discharge, river_discharge_cfs],
    matching the schema produced by the USGS NWIS pull in get_usgs_discharge_data.py.
    """
    df = pd.read_csv(url, compression="infer")
    df = df[(df["variable"] == "Flow_cms_mean") & (df["site_id"].isin(site_map.keys()))]

    out = df[["datetime", "site_id", "observation"]].copy()
    out.columns = ["time", "site_id", "river_discharge"]
    out["time"] = pd.to_datetime(out["time"], errors="coerce").dt.tz_localize(None)
    out["site_id"] = out["site_id"].map(site_map)
    out["river_discharge"] = pd.to_numeric(out["river_discharge"], errors="coerce")
    out["river_discharge_cfs"] = out["river_discharge"] / 0.028316847

    out = out.dropna(subset=["time", "site_id"])
    out = out[(out["time"] >= pd.to_datetime(start_time)) & (out["time"] <= pd.to_datetime(end_time))]

    return out.sort_values(["site_id", "time"]).reset_index(drop=True)
