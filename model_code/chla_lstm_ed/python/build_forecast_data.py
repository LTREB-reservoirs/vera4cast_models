"""Build an encoder-decoder ``forecast_data`` netCDF for a single reference date.

The encoder-decoder BMI forecast path (``bmi_lstm.forecast_encoder_decoder``)
loads ``config['forecast_data_file']`` and needs *two* things from it that the
training pipeline keeps in separate objects:

  1. A window of past *observations* for the encoder (``_build_encoder_input``).
  2. Future *met forecasts* (+ PI90 spread) for the decoder
     (``_build_decoder_input``).

``data_prep_encoder_decoder`` never writes this file, so this script assembles
it on demand for one reference date, pulling operational GEFS met drivers from
FLARE-forecast's stage2 archive (one file per ``reference_datetime``/``site_id``,
31 ensemble members -> real forecast uncertainty).

Real-time met lag (``met_driver_lag_days``, default 1): the current day's met
drivers are not available at forecast time -- stage2 publishes
``reference_datetime`` at least a day behind real time -- so a forecast
initialized on date ``R`` is driven by the most recent available driver,
initialized ``R - met_driver_lag_days``. The decoder horizon is sliced so its lead
time 0 lands on ``R`` (the forecast's first day). Set the lag to 0 only for
hindcasts, where the same-day driver does exist.

The encoder's ``river_discharge`` feature is likewise pulled live, from the
VERA4cast inflow target (``vera4cast_inflow_url`` + ``vera4cast_inflow_site_map``),
so it isn't capped at the training ``max_date`` the way the staged
``hydro_local_file`` is. Configure only ``hydro_local_file`` to fall back to the
staged file.

Output layout (single file, raw/unscaled units -- the BMI scales at inference):

    coords:
      time              : daily, [R-enc_len .. R-1]     # encoder lookback window (the
                          enc_len days before R, as in training)
      lead_time         : timedelta64, 0d .. (dec_len-1)d  # decoder horizon (R + l);
                          driven by the stage2 driver init at R - met_driver_lag_days
    scalar coord / attrs:
      reference_datetime = R
    data_vars:
      <encoder var>          (time, site_id)        # e.g. temperature_2m, river_discharge, elev
      <met var>_forecast     (lead_time, site_id)   # ensemble median forecast
      <met var>_pi90         (lead_time, site_id)   # Q95 - Q05 ensemble spread

Notes on variables:
  * ``<met var>`` for the value and ``<met var>_pi90`` for the spread avoids the
    encoder/decoder name collision on ``temperature_2m`` etc.  ``_pi90`` keeps
    its decoder name unchanged.
  * FLARE's stage2/stage3 archives have no ``total_cloud_cover_atmosphere``
    equivalent. With ``cloud_cover_source: "dynamical"`` it is filled from the
    dynamical.org GEFS analysis (encoder) and 35-day forecast (decoder); otherwise
    it (and its PI90) is written as NaN.

How ``src/torch_bmi.py`` reads this file:
  * ``_build_encoder_input``: the ``encoder_seq_len`` days ending the day before
    ``reference_datetime`` (all of ``time``).
  * ``_build_decoder_input``: ``f"{var}_forecast"`` / ``f"{var}_pi90"`` over
    ``lead_time`` (0 .. decoder_seq_len-1).
  * ``_set_reference_time``: ``get_current_date()`` returns ``reference_datetime``.

Usage:
    python build_forecast_data.py model_config.yml 2026-10-06
    python build_forecast_data.py model_config.yml 2026-10-06 --out in/forecast_data/20261006.nc
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.dynamical_utils import pull_gefs_analysis
from src.flare_s3_utils import pull_gefs_analysis_flare, pull_gefs_operational_flare
from src.helper_utils import (
    chla_lag_days,
    chla_lag_source,
    get_model_id,
    load_config,
    met_driver_lag_days,
)
from src.metadata_utils import get_metadata
from src.pull_vera4cast_chla_data import build_chla_lag, chla_lag_url
from src.pull_vera4cast_inflow_data import pull_vera4cast_inflow
from src.prep_data import (
    aggregate_analysis_gefs,
    aggregate_operational_gefs,
    calculate_forecast_uncertainty,
)
from src.pull_static_data import pull_static_features

_FLARE_SOURCES = ("flare_s3_stage2", "flare_s3_stage3")


def _encoder_vars(config: dict) -> list[str]:
    """Encoder feature order -- must match the trained ``encoder_vars``."""
    vars_ = list(config["x_vars"])
    vars_ += list(config.get("hydro_vars") or [])
    vars_ += list(config["x_vars_static"])
    if chla_lag_source(config):
        vars_ += ["chla_lagged", "chla_uncertainty_lagged"]
    return vars_


def _clean_attrs(ds: xr.Dataset) -> xr.Dataset:
    """Drop coords attrs that netCDF/CF cannot encode."""
    for name in ("time", "lead_time", "init_time", "reference_datetime"):
        if name in ds.coords:
            ds[name].attrs = {}
    return ds


def _pull_analysis(config: dict, site_metadata: xr.Dataset, start, end, variables):
    """Historical/analysis met drivers, dispatched on ``met_data_source``."""
    source = config.get("met_data_source", "dynamical")
    if source in _FLARE_SOURCES:
        return pull_gefs_analysis_flare(
            start_time=start, end_time=end, site_metadata=site_metadata, variables=variables,
            cloud_cover_source=config.get("cloud_cover_source"),
            email=config.get("email", "optional@email.com"),
        ).load()
    return pull_gefs_analysis(
        start_time=start,
        end_time=end,
        site_metadata=site_metadata,
        variables=variables,
        email=config["email"],
    ).load()


def _build_encoder_hydro_local(config: dict, start, ref) -> xr.Dataset:
    """Staged river discharge from ``hydro_local_file`` (fallback path)."""
    hydro_path = config["hydro_local_file"]
    if not Path(hydro_path).exists():
        raise FileNotFoundError(
            f"Hydro file {hydro_path} not found. Run get_usgs_discharge_data.py, "
            "or re-pull it to cover the requested reference date."
        )
    hydro_full = xr.load_dataset(hydro_path, engine="netcdf4")
    h_min, h_max = hydro_full.time.min().values, hydro_full.time.max().values
    if start < h_min or ref > h_max:
        print(
            f"WARNING: {hydro_path} covers {h_min} .. {h_max} but the encoder window is "
            f"{start} .. {ref}. Re-pull it (get_usgs_discharge_data.py) to cover this "
            "reference date, or the river_discharge encoder feature will be NaN."
        )
    return hydro_full.sel(time=slice(start, ref))


def _build_encoder_hydro(config: dict, start, ref) -> xr.Dataset:
    """Daily (time, site_id) river discharge over the encoder window.

    Prefers a live VERA4cast inflow pull (``vera4cast_inflow_url`` +
    ``vera4cast_inflow_site_map``), mirroring how the met drivers are pulled at
    build time. Falls back to the staged ``hydro_local_file`` when no inflow
    source is configured (e.g. USGS-gauge site sets).
    """
    inflow_url = config.get("vera4cast_inflow_url")
    inflow_map = config.get("vera4cast_inflow_site_map")
    if not (inflow_url and inflow_map):
        print(f"No vera4cast_inflow_url/site_map configured; using {config['hydro_local_file']}")
        return _build_encoder_hydro_local(config, start, ref)

    print(f"Pulling live VERA4cast inflow discharge for encoder window {start} .. {ref}")
    df = pull_vera4cast_inflow(
        url=inflow_url, site_map=inflow_map, start_time=start, end_time=ref
    )
    if df.empty:
        raise ValueError(
            f"VERA4cast inflow pull returned no rows for {pd.Timestamp(start).date()} .. "
            f"{pd.Timestamp(ref).date()}; check vera4cast_inflow_url/site_map."
        )

    wide = (
        df.pivot_table(index="time", columns="site_id", values="river_discharge", aggfunc="mean")
        .sort_index()
        .sort_index(axis=1)
    )
    n_obs = len(wide)
    # The inflow target lags real time by a few days and has interior gaps; fill
    # the same way the staged file did (linear, <=7 days, both directions) so the
    # tail of the encoder window isn't NaN.
    window = pd.date_range(pd.Timestamp(start), pd.Timestamp(ref), freq="D")
    wide = wide.reindex(window).interpolate(method="linear", limit=7, limit_direction="both")

    n_nan = int(wide.isna().to_numpy().sum())
    if n_nan:
        raise ValueError(
            f"river_discharge has {n_nan} unfilled day(s) in the encoder window "
            f"{pd.Timestamp(start).date()} .. {pd.Timestamp(ref).date()} (inflow source "
            f"covers {n_obs} of {len(window)} days). Extend the pull before building."
        )

    last = pd.Timestamp(df["time"].max())
    if last < pd.Timestamp(ref):
        print(
            f"NOTE: inflow source ends {last.date()}, "
            f"{int((pd.Timestamp(ref) - last).days)} day(s) before the reference date; "
            "those tail days hold the last observed value."
        )

    return xr.Dataset(
        {"river_discharge": (("time", "site_id"), wide.to_numpy())},
        coords={"time": wide.index.to_numpy(), "site_id": [str(s) for s in wide.columns]},
    )


def _build_encoder_history(config: dict, site_metadata: xr.Dataset, ref: np.datetime64, enc_len: int):
    """Daily (time, site_id) history over the encoder lookback window.

    The window is the ``enc_len`` days *before* the reference date (R-enc_len ..
    R-1), matching ``create_encoder_decoder_samples``, where the encoder ends the
    day before the first forecast/target day.
    """
    end = ref - np.timedelta64(1, "D")
    start = ref - np.timedelta64(enc_len, "D")

    print(f"Pulling analysis met for encoder window {start} .. {end}")
    # The pull is inclusive of the whole end day, so R-1 gets all 24 hours.
    analysis = _pull_analysis(config, site_metadata, start, end, config["x_vars"])
    analysis = analysis.interpolate_na(dim="time", method="linear")
    analysis_daily = aggregate_analysis_gefs(ds=analysis, out_vars=config["x_vars"])
    # aggregate_* resamples to day-start labels; keep the requested inclusive window.
    analysis_daily = analysis_daily.sel(time=slice(start, end))
    cloud = "total_cloud_cover_atmosphere"
    if config.get("cloud_cover_source") and cloud in analysis_daily:
        # The dynamical.org analysis can trail real time by a few hours; carry the last
        # value forward so a short gap at the end of the window doesn't leave a
        # partially-NaN feature (an all-NaN one still falls back to the training mean).
        n_gap = int(analysis_daily[cloud].isnull().sum())
        analysis_daily[cloud] = analysis_daily[cloud].ffill("time", limit=3)
        if n_gap and not bool(analysis_daily[cloud].isnull().all()):
            print(f"NOTE: forward-filled {n_gap} missing daily {cloud} value(s) at the end "
                  "of the encoder window.")

    hydro = None
    if config.get("hydro_vars"):
        hydro = _build_encoder_hydro(config, start, end)

    return analysis_daily, hydro


def _build_decoder_forecast(config: dict, site_metadata: xr.Dataset, ref: np.datetime64, dec_len: int,
                            lag_days: int = 1):
    """Ensemble median + PI90 over the decoder horizon as (lead_time, site_id).

    The met driver is initialized at ``init = ref - lag_days`` because the
    current day's drivers are not available at forecast time; the decoder horizon
    is therefore sliced from ``lag_days`` so its lead time 0 lands on ``ref`` (the
    forecast's first day) rather than on the driver's own init day.
    """
    source = config.get("met_data_source", "dynamical")
    if source != "flare_s3_stage2":
        print(
            f"WARNING: met_data_source is '{source}', not 'flare_s3_stage2'. "
            "Decoder uncertainty will not reflect real GEFS ensemble spread."
        )

    init = ref - np.timedelta64(lag_days, "D")
    print(f"Pulling stage2 operational GEFS initialized {init} "
          f"(reference date {ref} minus {lag_days} day lag)")

    # The horizon must still reach the last forecast day after the lag shift, so
    # request at least lag + dec_len - 1 days of lead time regardless of config.
    needed_days = lag_days + dec_len - 1
    lead_times = config["lead_times"]
    if pd.Timedelta(lead_times) < pd.Timedelta(days=needed_days):
        print(f"NOTE: extending lead_times from '{lead_times}' to '{needed_days}d' to "
              f"cover the {lag_days}-day driver lag plus a {dec_len}-day horizon.")
        lead_times = f"{needed_days}d"

    operational = pull_gefs_operational_flare(
        start_time=init,
        end_time=init,
        site_metadata=site_metadata,
        lead_times=lead_times,
        variables=config["x_vars"],
        cloud_cover_source=config.get("cloud_cover_source"),
        email=config.get("email", "optional@email.com"),
    ).load()
    operational_daily = aggregate_operational_gefs(ds=operational, out_vars=config["x_vars"])

    pi90 = calculate_forecast_uncertainty(
        ds=operational_daily, met_vars=config["x_vars"], include_median=False
    )

    # Single init date -> collapse the init_time axis.
    ref_sel = operational_daily.sel(init_time=init)
    lead_slice = slice(pd.Timedelta(days=lag_days), pd.Timedelta(days=lag_days + dec_len - 1))
    n_lead = operational_daily.sizes["lead_time"]
    if len(operational_daily.lead_time.sel(lead_time=lead_slice)) < dec_len:
        print(
            f"WARNING: stage2 horizon ({n_lead} hourly steps) yields fewer than "
            f"{dec_len} daily lead times from day {lag_days}; decoder will be short."
        )

    met_median = ref_sel.median(dim="ensemble_member").sel(lead_time=lead_slice)
    pi90_sel = pi90.sel(init_time=init).sel(lead_time=lead_slice)

    # Relabel the horizon onto the forecast's own clock: the driver day that lands
    # on R becomes lead time 0, so lead time l always means R + l days. The model
    # reads these by position, but the coordinate keeps the file self-describing.
    new_lead = pd.timedelta_range(start="0d", periods=met_median.sizes["lead_time"], freq="1D")
    met_median = met_median.assign_coords(lead_time=new_lead)
    pi90_sel = pi90_sel.assign_coords(lead_time=new_lead)

    # Guard against a stage2 hole filling the whole horizon with NaN (the decoder
    # would otherwise silently fall back to scaler means for every met feature).
    n_days = met_median.sizes["lead_time"]
    all_nan_vars = [
        v for v in config["x_vars"]
        if bool(np.isnan(met_median[v]).all())
    ]
    # total_cloud_cover_atmosphere is legitimately all-NaN (no FLARE equivalent)
    # and is handled downstream, so only flag it when every var is missing.
    unsupported = {"total_cloud_cover_atmosphere"}
    if all_nan_vars and set(all_nan_vars) - unsupported:
        raise ValueError(
            f"No stage2 driver values for init {init} over the horizon "
            f"{lead_slice.start} .. {lead_slice.stop} ({n_days} day(s)): "
            f"all-NaN for {all_nan_vars}. The reference date is likely ahead of "
            "the last published stage2 reference_datetime; re-run once the driver "
            "is available or lower met_driver_lag_days."
        )

    missing = [v for v in config["x_vars"] if bool(np.isnan(met_median[v]).all())]
    if missing:
        print(f"WARNING: these met vars are all-NaN for {ref} (no FLARE equivalent): {missing}")

    return met_median, pi90_sel


def _build_chla_lag_encoder(config: dict, time_index, site_ids: list):
    """Lagged-chla block over the encoder lookback window, on ``time_index``.

    Pulls observed VERA4cast ``Chla_ugL_mean`` (the ``chla_lag`` source) and shifts
    it by ``lag_days`` so day ``t`` holds chla observed at ``t - lag_days``.
    """
    lag = build_chla_lag(
        url=chla_lag_url(config),
        start_time=pd.Timestamp(time_index.min()),
        end_time=pd.Timestamp(time_index.max()),
        lag_days=int(chla_lag_days(config)),
        site_ids=site_ids,
    )
    return lag.reindex(time=time_index)


def build(config: dict, ref_date: str, out_path: str | None = None) -> Path:
    ref = np.datetime64(pd.Timestamp(ref_date).normalize())
    enc_len = int(config["encoder_seq_len"])
    dec_len = int(config["decoder_seq_len"])

    met_lag = met_driver_lag_days(config)
    if met_lag:
        print(f"Met drivers lagged {met_lag} day(s) behind the reference date "
              f"(met_driver_lag_days); decoder init = {ref - np.timedelta64(met_lag, 'D')}")

    lag_source = chla_lag_source(config)
    if lag_source == "noAR":
        raise NotImplementedError(
            "lag_target=True needs lagged chla history (obs + noAR predictions) for the "
            "encoder; wire that in before using this script for an AR model."
        )

    site_metadata = get_metadata(
        config["site_metadata_url"],
        config["savoy_metadata_file"],
        config["stackpoole_metadata_file"],
        sites=config["sites_to_include"],
    )
    if config.get("site_ids_to_include"):
        keep = list(config["site_ids_to_include"])
        site_metadata = site_metadata.sel(site_id=[s for s in site_metadata.site_id.values if s in keep])
    site_ids = [str(s) for s in site_metadata.site_id.values]
    print(f"Sites: {site_ids}")

    analysis_daily, hydro = _build_encoder_history(config, site_metadata, ref, enc_len)
    met_median, pi90_sel = _build_decoder_forecast(config, site_metadata, ref, dec_len, lag_days=met_lag)
    static = pull_static_features(site_metadata, config["x_vars_static"],
                                  data_in_dir=config.get("data_in_dir", "in/"))

    # Assemble encoder block on the daily lookback axis.
    encoder_ds = xr.Dataset()
    for var in config["x_vars"]:
        encoder_ds[var] = analysis_daily[var]
    if hydro is not None:
        for var in config["hydro_vars"]:
            encoder_ds[var] = hydro[var]
    for var in config["x_vars_static"]:
        encoder_ds[var] = static[var].broadcast_like(encoder_ds["time"])
    if lag_source == "realtime":
        print(f"Pulling real-time chla lag for encoder window ending {ref}")
        lagged = _build_chla_lag_encoder(config, encoder_ds["time"].to_index(), site_ids)
        encoder_ds["chla_lagged"] = lagged["chla_lagged"]
        encoder_ds["chla_uncertainty_lagged"] = lagged["chla_uncertainty_lagged"]

    # Assemble decoder block on the lead_time axis.
    decoder_ds = xr.Dataset()
    for var in config["x_vars"]:
        decoder_ds[f"{var}_forecast"] = met_median[var].transpose("lead_time", "site_id")
        decoder_ds[f"{var}_pi90"] = pi90_sel[f"{var}_pi90"].transpose("lead_time", "site_id")

    ds = xr.merge([encoder_ds, decoder_ds])
    # init_time is a leftover scalar coord from the per-reference-date selection.
    if "init_time" in ds.coords:
        ds = ds.drop_vars("init_time")
    ds = ds.assign_coords(reference_datetime=np.datetime64(ref))
    ds = ds.reindex({"site_id": site_ids})
    ds.attrs = {
        "model_id": get_model_id(config),
        "reference_datetime": str(pd.Timestamp(ref).date()),
        "encoder_seq_len": enc_len,
        "decoder_seq_len": dec_len,
        "met_data_source": config.get("met_data_source", "dynamical"),
        "met_driver_lag_days": met_lag,
    }
    ds = _clean_attrs(ds)

    out = Path(out_path or config["forecast_data_file"])
    out.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path=out, engine="netcdf4", mode="w")
    print(f"\nWrote forecast_data: {out}")
    print(f"  dims: {dict(ds.sizes)}")
    print(f"  encoder vars: {_encoder_vars(config)}")
    print(f"  decoder vars: {[f'{v}_forecast' for v in config['x_vars']]} + "
          f"{[f'{v}_pi90' for v in config['x_vars']]}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("config_file", nargs="?", default="model_config.yml")
    parser.add_argument("reference_date", help="forecast reference date, e.g. 2026-10-06")
    parser.add_argument("--out", default=None, help="override config['forecast_data_file']")
    args = parser.parse_args()

    config = load_config(args.config_file)
    build(config, args.reference_date, out_path=args.out)


if __name__ == "__main__":
    main()
