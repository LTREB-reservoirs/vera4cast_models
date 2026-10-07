from __future__ import annotations

from pathlib import Path

import pandas as pd
import xarray as xr


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lowered = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def pull_predicted_chl(data_file: str, start_time, end_time) -> xr.Dataset:
    """Load predicted chlorophyll values and return an xarray Dataset with variable chla."""
    path = Path(data_file)
    if not path.exists():
        raise FileNotFoundError(f"Predicted chlorophyll file not found: {data_file}")

    suffix = path.suffix.lower()
    if suffix == ".feather":
        df = pd.read_feather(path)
    elif suffix == ".parquet":
        df = pd.read_parquet(path)
    elif suffix in {".csv", ".txt"}:
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported predicted chlorophyll file format: {path.suffix}")

    time_col = _pick_column(df, ["time", "datetime", "date"])
    site_col = _pick_column(df, ["site_id", "site", "location_id"])
    value_col = _pick_column(
        df,
        ["chla", "chla_pred", "prediction", "pred", "yhat", "median", "chla_mu"],
    )

    if time_col is None or site_col is None or value_col is None:
        raise ValueError(
            "Could not identify required columns in predicted chlorophyll file. "
            f"Found columns: {list(df.columns)}"
        )

    out = df[[time_col, site_col, value_col]].copy()
    out.columns = ["time", "site_id", "chla"]
    out["time"] = pd.to_datetime(out["time"], errors="coerce").dt.tz_localize(None)
    out["site_id"] = out["site_id"].astype(str).str.strip()
    out["chla"] = pd.to_numeric(out["chla"], errors="coerce")
    out = out.dropna(subset=["time", "site_id"]) 

    start_ts = pd.to_datetime(start_time)
    end_ts = pd.to_datetime(end_time)
    out = out[(out["time"] >= start_ts) & (out["time"] <= end_ts)]

    pivot = out.pivot_table(index="time", columns="site_id", values="chla", aggfunc="mean")
    pivot = pivot.sort_index().sort_index(axis=1)

    return xr.Dataset(
        data_vars={"chla": (("time", "site_id"), pivot.to_numpy())},
        coords={"time": pivot.index.to_numpy(), "site_id": pivot.columns.to_numpy()},
    )
