"""Pull real-time chlorophyll-a observations from the VERA4cast targets endpoint.

This is an alternative lag source to the noAR-model predictions used by
``lag_target`` (see ``pull_predicted_chl.py``). Rather than gap-filling with a
model, it reads the observed ``Chla_ugL_mean`` series directly and shifts it to
build the ``chla_lagged`` / ``chla_uncertainty_lagged`` encoder features gated by
the ``chla_lag`` config flag.

It is mainly useful for operational inference, where the encoder needs a recent
chla history that extends past the training observation file (``obs.nc``), which
is capped at the training ``max_date``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

# VERA4cast daily-insitu-targets variable name for mean chlorophyll-a (ug/L)
CHLA_VARIABLE = "Chla_ugL_mean"
# Observation uncertainty: constant 5% CV, converted to PI90 (Q95 - Q05 for a Gaussian)
_OBS_CV = 0.05
_PI90_FROM_SD = 3.29


def chla_lag_url(config: dict) -> str:
    """Real-time chla endpoint, falling back to the training target URL."""
    return config.get("vera4cast_chla_url") or config["target_url"]


def pull_vera4cast_chla(
        url: str,
        start_time,
        end_time,
        site_ids: list | None = None,
        variable: str = CHLA_VARIABLE,
) -> xr.Dataset:
    """Pull observed chlorophyll-a and return a (time, site_id) dataset with variable ``chla``.

    Parameters
    ----------
    url : str
        VERA4cast-style long-format targets file (``datetime``/``site_id``/``variable``/``observation``).
    start_time, end_time : date-like
        Inclusive date bounds (after any lag shift).
    site_ids : list, optional
        Restrict to these ``site_id`` values. When None, all sites in the file are returned.
    variable : str
        Variable name to extract (default ``Chla_ugL_mean``).
    """
    df = pd.read_csv(url, compression="infer")

    if "variable" not in df.columns:
        raise ValueError(f"Expected a 'variable' column in {url}; found {list(df.columns)}")

    variable_names = df["variable"].astype(str).str.strip()
    if variable not in set(variable_names):
        raise ValueError(
            f"Variable '{variable}' not found in {url}. "
            f"Available: {sorted(set(variable_names))[:20]}"
        )
    df = df[variable_names == variable]

    out = df[["datetime", "site_id", "observation"]].copy()
    out.columns = ["time", "site_id", "chla"]
    out["time"] = pd.to_datetime(out["time"], errors="coerce").dt.tz_localize(None)
    out["site_id"] = out["site_id"].astype(str).str.strip()
    out["chla"] = pd.to_numeric(out["chla"], errors="coerce")
    out = out.dropna(subset=["time", "site_id"])

    if site_ids is not None:
        keep = {str(s) for s in site_ids}
        out = out[out["site_id"].isin(keep)]

    start_ts = pd.to_datetime(start_time)
    end_ts = pd.to_datetime(end_time)
    out = out[(out["time"] >= start_ts) & (out["time"] <= end_ts)]

    pivot = (
        out.pivot_table(index="time", columns="site_id", values="chla", aggfunc="mean")
        .sort_index()
        .sort_index(axis=1)
    )

    return xr.Dataset(
        data_vars={"chla": (("time", "site_id"), pivot.to_numpy())},
        coords={"time": pivot.index.to_numpy(), "site_id": pivot.columns.to_numpy()},
    )


def build_chla_lag(
        url: str,
        start_time,
        end_time,
        lag_days: int = 1,
        site_ids: list | None = None,
        variable: str = CHLA_VARIABLE,
        max_fill_days: int = 7,
) -> xr.Dataset:
    """Build ``chla_lagged`` / ``chla_uncertainty_lagged`` features from real-time chla.

    The values are the observed ``Chla_ugL_mean`` shifted back by ``lag_days``, so
    that at time ``t`` the feature holds chla observed at ``t - lag_days``. The
    returned dataset covers ``[start_time, end_time]``; ``lag_days`` of extra
    history is pulled internally so the first requested day has a lagged value.

    Missing days (sensor gaps, or the latest days not yet published) are
    forward-filled from the last observation for up to ``max_fill_days``. The fill
    is causal -- it never uses later observations -- so training sees exactly what
    is available at forecast time; longer gaps stay NaN (filled with the training
    mean downstream).
    """
    lag_days = int(lag_days)
    start_ts = pd.to_datetime(start_time)
    end_ts = pd.to_datetime(end_time)

    chla = pull_vera4cast_chla(
        url=url,
        start_time=start_ts - pd.Timedelta(days=lag_days + max_fill_days),
        end_time=end_ts,
        site_ids=site_ids,
        variable=variable,
    )
    # Regular daily axis through end_time so trailing unpublished days exist to fill.
    full_time = pd.date_range(start_ts - pd.Timedelta(days=lag_days + max_fill_days), end_ts, freq="D")
    chla = chla.reindex(time=full_time).ffill("time", limit=max_fill_days)
    chla = chla.assign(chla_uncertainty=lambda ds: _PI90_FROM_SD * _OBS_CV * np.abs(ds["chla"]))

    lagged = (
        chla[["chla", "chla_uncertainty"]]
        .shift(time=lag_days)
        .rename({"chla": "chla_lagged", "chla_uncertainty": "chla_uncertainty_lagged"})
    )
    return lagged.sel(time=slice(start_ts, end_ts))
