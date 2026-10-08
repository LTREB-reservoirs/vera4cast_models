"""Pull GEFS meteorological drivers from FLARE-forecast's pre-extracted per-site
parquet archive on the public OSN bucket, as a faster alternative to pulling
directly from the raw dynamical.org zarr stores (see dynamical_utils.py).

Each file here is already subset to a single site, so there is no spatial/ensemble
read-amplification like there is with the coarsely-chunked dynamical.org zarr stores.

Bucket layout (anonymous, S3-compatible, endpoint amnh1.osn.mghpcc.org):
  flare/drivers/met/gefs-v12/stage2/reference_datetime=<date>/site_id=<site>/part-0.parquet
      -- one file per (init date, site): ensemble forecast, ~35-day horizon.
         Equivalent of dynamical_utils.pull_gefs_operational.
  flare/drivers/met/gefs-v12/stage3/site_id=<site>/part-0.parquet
      -- one file per site: deterministic historical series, replicated across
         all 31 ensemble "parameter" ids for schema compatibility with stage2.
         Equivalent of dynamical_utils.pull_gefs_analysis.

Coverage is limited to the specific lake sites FLARE operates forecasts for
(e.g. "fcre", "bvre") -- site_ids with no match in the bucket are skipped with
a warning rather than raising, since not every site in this codebase is covered.

FLARE carries no cloud cover. With ``cloud_cover_source="dynamical"`` the pullers
fill ``total_cloud_cover_atmosphere`` from dynamical.org's NOAA GEFS zarr stores
instead (analysis for stage3, the 35-day forecast for stage2) -- a single variable
at a single point, which reads quickly despite those stores' coarse chunking.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import xarray as xr

FLARE_BUCKET = "bio230121-bucket01"
FLARE_ENDPOINT = "amnh1.osn.mghpcc.org"
STAGE2_PREFIX = "flare/drivers/met/gefs-v12/stage2"
STAGE3_PREFIX = "flare/drivers/met/gefs-v12/stage3"

# FLARE's CF-standard variable names -> dynamical.org GEFS names used throughout this codebase
FLARE_TO_DYNAMICAL_VARS = {
    "air_temperature": "temperature_2m",
    "relative_humidity": "relative_humidity_2m",
    "air_pressure": "pressure_surface",
    "eastward_wind": "wind_u_10m",
    "northward_wind": "wind_v_10m",
    "surface_downwelling_longwave_flux_in_air": "downward_long_wave_radiation_flux_surface",
    "surface_downwelling_shortwave_flux_in_air": "downward_short_wave_radiation_flux_surface",
    "precipitation_flux": "precipitation_surface",
}
# FLARE's stage2/stage3 archive has no equivalent for these -- filled with NaN if requested
_UNSUPPORTED_VARS = {"total_cloud_cover_atmosphere"}

CLOUD_VAR = "total_cloud_cover_atmosphere"
DYNAMICAL_GEFS_URL = "https://data.dynamical.org/noaa/gefs/{store}/latest.zarr?email={email}"


def _s3_filesystem(endpoint: str = FLARE_ENDPOINT) -> "pafs.S3FileSystem":
    return pafs.S3FileSystem(endpoint_override=endpoint, anonymous=True, scheme="https")


def _read_parquet(fs, path):
    try:
        return pq.read_table(
            path, filesystem=fs, columns=["parameter", "datetime", "variable", "prediction"]
        ).to_pandas()
    except FileNotFoundError:
        return None


def _apply_var_mapping_and_units(df: pd.DataFrame) -> pd.DataFrame:
    """Map FLARE variable names to dynamical.org names and fix units."""
    df = df.copy()
    df["variable"] = df["variable"].map(FLARE_TO_DYNAMICAL_VARS).fillna(df["variable"])
    # FLARE stores air_temperature in Kelvin (+273 from raw Celsius); this codebase's
    # x_vars (temperature_2m, maximum/minimum_temperature_2m) are in Celsius.
    is_temp = df["variable"] == "temperature_2m"
    df.loc[is_temp, "prediction"] = df.loc[is_temp, "prediction"] - 273
    return df


def _finalize_met_vars(ds: xr.Dataset, variables: list, filled_elsewhere=()) -> xr.Dataset:
    """Add derived/missing variables so the output matches the requested variable set.

    Variables in ``filled_elsewhere`` get a NaN placeholder without a warning; the
    caller fills them from another source afterwards.
    """
    if "temperature_2m" in ds:
        # FLARE only provides a single hourly temperature series (no separate daily
        # max/min variables); derive them here so the existing aggregate_*_gefs()
        # resample('1d').max()/.min() logic downstream works unchanged.
        if "maximum_temperature_2m" in variables and "maximum_temperature_2m" not in ds:
            ds["maximum_temperature_2m"] = ds["temperature_2m"]
        if "minimum_temperature_2m" in variables and "minimum_temperature_2m" not in ds:
            ds["minimum_temperature_2m"] = ds["temperature_2m"]

    template = ds[list(ds.data_vars)[0]]
    for var in variables:
        if var not in ds:
            if var in filled_elsewhere:
                pass
            elif var in _UNSUPPORTED_VARS:
                print(f"WARNING: FLARE S3 met source has no '{var}' equivalent; filling with NaN.")
            else:
                print(f"WARNING: requested variable '{var}' not found in FLARE met data; filling with NaN.")
            ds[var] = xr.full_like(template, np.nan)

    return ds[variables]


def _dynamical_cloud_points(store: str, email: str, site_metadata: xr.Dataset) -> xr.DataArray:
    """Lazy ``total_cloud_cover_atmosphere`` at each site's nearest GEFS grid point.

    Returned with a ``site_id`` dimension (from ``site_metadata``), in percent.
    """
    ds = xr.open_zarr(DYNAMICAL_GEFS_URL.format(store=store, email=email),
                      chunks=None, decode_timedelta=True)
    da = ds[CLOUD_VAR].sel(
        latitude=site_metadata.latitude, longitude=site_metadata.longitude, method="nearest"
    )
    # Keep only dimension coordinates: dynamical.org adds extras (valid_time,
    # expected_forecast_length, spatial_ref, ...) that make chunks pulled with and
    # without cloud cover impossible to concatenate.
    return da.reset_coords(drop=True).assign_coords(
        site_id=[str(s) for s in site_metadata.site_id.values]
    )


def _plain(da: xr.DataArray) -> xr.DataArray:
    """Drop dynamical.org's variable/coordinate attrs (some are dicts, e.g.
    ``statistics_approximate``, which netCDF can't serialize) before merging."""
    da = da.copy()
    da.attrs = {"units": "percent"}
    for name in da.coords:
        da[name].attrs = {}
    return da


def _fill_cloud_analysis(ds: xr.Dataset, site_metadata: xr.Dataset, email: str) -> xr.Dataset:
    """Fill ``total_cloud_cover_atmosphere`` on a (time, site_id) stage3 pull from the
    dynamical.org GEFS analysis (3-hourly; hours in between stay NaN, which the daily
    aggregation and the inference-time interpolation both handle)."""
    t0, t1 = ds.time.values.min(), ds.time.values.max()
    cloud = _dynamical_cloud_points("analysis", email, site_metadata.sel(site_id=ds.site_id))
    cloud = cloud.sel(time=slice(t0, t1)).load()
    ds[CLOUD_VAR] = _plain(cloud.reindex(time=ds.time, site_id=ds.site_id).transpose(*ds[CLOUD_VAR].dims))
    print(f"Filled {CLOUD_VAR} from the dynamical.org GEFS analysis "
          f"({int(np.isfinite(cloud.values).sum())} values).")
    return ds


def _fill_cloud_operational(ds: xr.Dataset, site_metadata: xr.Dataset, email: str,
                            max_workers: int = 8) -> xr.Dataset:
    """Fill ``total_cloud_cover_atmosphere`` on a stage2 pull (init_time, lead_time,
    ensemble_member, site_id) from the dynamical.org GEFS 35-day forecast for the same
    00z init dates. The forecast is 3-hourly (6-hourly after day 10), so hourly leads
    in between stay NaN; daily aggregation averages the available steps. Members are
    matched by index, which is harmless since only the ensemble median/spread is used.
    """
    cloud = _dynamical_cloud_points("forecast-35-day", email, site_metadata.sel(site_id=ds.site_id))
    cloud = cloud.sel(lead_time=slice(ds.lead_time.values.min(), ds.lead_time.values.max()))
    available = set(pd.DatetimeIndex(cloud.init_time.values))
    inits = [t for t in pd.DatetimeIndex(ds.init_time.values) if t in available]

    def _load(t, attempts=4):
        # dynamical.org reads occasionally drop mid-response (ContentLengthError);
        # retry with a short backoff before giving up on this init date.
        for attempt in range(attempts):
            try:
                return cloud.sel(init_time=t).load()
            except Exception:
                if attempt == attempts - 1:
                    raise
                time.sleep(2 * (attempt + 1))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        pieces = list(pool.map(_load, inits))
    n_missing = ds.sizes["init_time"] - len(inits)
    if n_missing:
        print(f"WARNING: {n_missing} init date(s) missing from the dynamical.org forecast; "
              f"{CLOUD_VAR} stays NaN for them.")
    if pieces:
        cloud = xr.concat(pieces, dim="init_time").assign_coords(
            ensemble_member=lambda c: c.ensemble_member.astype(int)
        )
        ds[CLOUD_VAR] = _plain(cloud.reindex_like(ds[CLOUD_VAR]).transpose(*ds[CLOUD_VAR].dims))
    print(f"Filled {CLOUD_VAR} from the dynamical.org GEFS 35-day forecast "
          f"({len(inits)} init date(s)).")
    return ds


def _fill_cloud(ds, variables, cloud_cover_source, fill_fn, site_metadata, email,
                required=False):
    """Fill cloud cover from ``cloud_cover_source`` when requested.

    On failure, ``required=False`` (operational forecasts) keeps the NaN placeholder,
    handled downstream as an unavailable feature, and warns; ``required=True``
    (training data pulls) re-raises so the caller retries instead of saving a gap.
    """
    if cloud_cover_source != "dynamical" or CLOUD_VAR not in variables:
        return ds
    try:
        return fill_fn(ds, site_metadata, email)
    except Exception as e:
        if required:
            raise
        print(f"WARNING: could not fill {CLOUD_VAR} from dynamical.org "
              f"({type(e).__name__}: {e}); leaving it NaN.")
        return ds


def pull_gefs_analysis_flare(
        start_time: np.datetime64,
        end_time: np.datetime64,
        site_metadata: xr.Dataset,
        variables: list,
        bucket: str = FLARE_BUCKET,
        endpoint: str = FLARE_ENDPOINT,
        cloud_cover_source: str | None = None,
        email: str = "optional@email.com",
        cloud_cover_required: bool = False,
) -> xr.Dataset:
    """
    Retrieves historical GEFS met drivers from FLARE-forecast's stage3 parquet
    archive. Drop-in alternative to `dynamical_utils.pull_gefs_analysis`.

    Parameters mirror `dynamical_utils.pull_gefs_analysis`; `bucket`/`endpoint`
    replace `base_url` since this reads parquet from S3 instead of zarr.
    `cloud_cover_source="dynamical"` fills cloud cover from the dynamical.org GEFS
    analysis (`email` is passed to dynamical.org for usage tracking).
    `cloud_cover_required=True` raises if that fill fails instead of leaving NaN.
    """
    filled_elsewhere = (CLOUD_VAR,) if cloud_cover_source == "dynamical" else ()
    fs = _s3_filesystem(endpoint)
    start_ts = pd.Timestamp(str(start_time), tz="UTC")
    end_ts = pd.Timestamp(str(end_time), tz="UTC") + pd.Timedelta(days=1)

    site_datasets = []
    for site_id in site_metadata.site_id.values:
        site_id = str(site_id)
        path = f"{bucket}/{STAGE3_PREFIX}/site_id={site_id}/part-0.parquet"
        df = _read_parquet(fs, path)
        if df is None:
            print(f"WARNING: no FLARE stage3 met data found for site_id={site_id}; skipping.")
            continue

        df = df[(df["datetime"] >= start_ts) & (df["datetime"] < end_ts)]
        # series is deterministic, replicated across all 31 parameter ids -- keep one copy
        df = df[df["parameter"] == df["parameter"].min()]
        df = _apply_var_mapping_and_units(df)
        # netCDF/CF encoding can't handle tz-aware datetime64; this codebase uses naive UTC
        df["datetime"] = df["datetime"].dt.tz_localize(None)

        # A handful of timestamps appear twice in the upstream stage3 archive (e.g. fcre,
        # 2025-07-22..25). pivot() raises on duplicate index entries, so drop the extras.
        n_dup = int(df.duplicated(subset=["datetime", "variable"]).sum())
        if n_dup:
            print(f"WARNING: dropping {n_dup} duplicate (datetime, variable) row(s) "
                  f"for site_id={site_id}.")
            df = df.drop_duplicates(subset=["datetime", "variable"], keep="first")

        wide = df.pivot(index="datetime", columns="variable", values="prediction")
        wide.index.name = "time"
        site_ds = _finalize_met_vars(xr.Dataset.from_dataframe(wide), variables, filled_elsewhere)
        site_datasets.append(site_ds.expand_dims(site_id=[site_id]))

    if not site_datasets:
        raise RuntimeError("No FLARE stage3 met data found for any requested site_id.")

    ds = xr.concat(site_datasets, dim="site_id")
    return _fill_cloud(ds, variables, cloud_cover_source, _fill_cloud_analysis, site_metadata, email,
                       required=cloud_cover_required)


def pull_gefs_operational_flare(
        start_time: np.datetime64,
        end_time: np.datetime64,
        site_metadata: xr.Dataset,
        lead_times: str,
        variables: list,
        bucket: str = FLARE_BUCKET,
        endpoint: str = FLARE_ENDPOINT,
        max_workers: int = 8,
        cloud_cover_source: str | None = None,
        email: str = "optional@email.com",
        cloud_cover_required: bool = False,
) -> xr.Dataset:
    """
    Retrieves ensemble GEFS forecast met drivers from FLARE-forecast's stage2
    parquet archive (one file per reference_datetime/site_id, 31 members).
    Drop-in alternative to `dynamical_utils.pull_gefs_operational`.
    `cloud_cover_source="dynamical"` fills cloud cover from the dynamical.org GEFS
    35-day forecast for the same init dates. `cloud_cover_required=True` raises if
    that fill fails instead of leaving NaN.
    """
    filled_elsewhere = (CLOUD_VAR,) if cloud_cover_source == "dynamical" else ()
    fs = _s3_filesystem(endpoint)
    lead_time_limit = pd.Timedelta(lead_times)
    ref_dates = pd.date_range(str(start_time), str(end_time), freq="1D", tz="UTC")
    site_ids = [str(s) for s in site_metadata.site_id.values]

    def _fetch_one(site_id, ref_date):
        ref_str = ref_date.strftime("%Y-%m-%d")
        path = f"{bucket}/{STAGE2_PREFIX}/reference_datetime={ref_str}/site_id={site_id}/part-0.parquet"
        df = _read_parquet(fs, path)
        if df is None:
            return site_id, None
        df["lead_time"] = df["datetime"] - ref_date
        df = df[df["lead_time"] <= lead_time_limit]
        # netCDF/CF encoding can't handle tz-aware datetime64; this codebase uses naive UTC
        df["init_time"] = ref_date.tz_localize(None)
        return site_id, df

    fetched = {site_id: [] for site_id in site_ids}
    n_missing = {site_id: 0 for site_id in site_ids}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_fetch_one, s, d) for s in site_ids for d in ref_dates]
        for fut in as_completed(futures):
            site_id, df = fut.result()
            if df is None:
                n_missing[site_id] += 1
            else:
                fetched[site_id].append(df)

    site_datasets = []
    for site_id in site_ids:
        if n_missing[site_id]:
            print(f"WARNING: {n_missing[site_id]}/{len(ref_dates)} stage2 reference_datetime(s) "
                  f"missing for site_id={site_id}.")
        if not fetched[site_id]:
            print(f"WARNING: no FLARE stage2 forecast data found for site_id={site_id}; skipping.")
            continue

        df = pd.concat(fetched[site_id], ignore_index=True)
        df = _apply_var_mapping_and_units(df)

        wide = df.pivot(index=["init_time", "lead_time", "parameter"], columns="variable", values="prediction")
        site_ds = xr.Dataset.from_dataframe(wide).rename({"parameter": "ensemble_member"})
        site_ds = site_ds.assign_coords(ensemble_member=site_ds.ensemble_member.astype(int))
        site_ds = _finalize_met_vars(site_ds, variables, filled_elsewhere)
        site_datasets.append(site_ds.expand_dims(site_id=[site_id]))

    if not site_datasets:
        raise RuntimeError("No FLARE stage2 forecast data found for any requested site_id.")

    ds = xr.concat(site_datasets, dim="site_id")
    return _fill_cloud(ds, variables, cloud_cover_source, _fill_cloud_operational, site_metadata, email,
                       required=cloud_cover_required)


def pull_gefs_operational_from_stage3(
        start_time: np.datetime64,
        end_time: np.datetime64,
        site_metadata: xr.Dataset,
        lead_times: str,
        variables: list,
        n_members: int = 31,
        bucket: str = FLARE_BUCKET,
        endpoint: str = FLARE_ENDPOINT,
) -> xr.Dataset:
    """
    Builds a pseudo-"operational" forecast dataset by locally windowing FLARE's
    stage3 historical record, instead of pulling real forecast ensembles from
    stage2. Each "forecast" window is just the actual historical trajectory for
    the following `lead_times` (zero ensemble spread, replicated across
    `n_members` only for shape-compatibility with the stage2-based pipeline).

    Requires a single parquet read per site (same as `pull_gefs_analysis_flare`)
    instead of one S3 request per reference_datetime, so it is dramatically
    faster than `pull_gefs_operational_flare` -- at the cost of not reflecting
    real GEFS forecast skill/uncertainty (every "member" is identical).
    """
    lead_time_limit = pd.Timedelta(lead_times)
    # fetch extra history past end_time so the last init_time's forecast window is covered
    fetch_end = np.datetime64(pd.Timestamp(str(end_time)) + lead_time_limit + pd.Timedelta(days=1))

    hourly_ds = pull_gefs_analysis_flare(
        start_time=start_time,
        end_time=fetch_end,
        site_metadata=site_metadata,
        variables=variables,
        bucket=bucket,
        endpoint=endpoint,
    )

    # Reindex onto a strictly regular hourly grid so the (init_time, lead_time) window
    # for every init_time can be built with a single vectorized fancy-index lookup
    # instead of looping + concatenating per day (which doesn't scale to years of data).
    time_index = hourly_ds.indexes["time"]  # tz-aware pandas DatetimeIndex
    full_hourly_index = pd.date_range(time_index.min(), time_index.max(), freq="1h", tz=time_index.tz)
    hourly_ds = hourly_ds.reindex(time=full_hourly_index)

    init_times = pd.date_range(str(start_time), str(end_time), freq="1D", tz=time_index.tz)
    lead_time_vals = pd.timedelta_range(start="0h", end=lead_time_limit, freq="1h")

    init_pos = full_hourly_index.get_indexer(init_times)
    lead_pos = np.arange(len(lead_time_vals))
    # gather_idx[i, j] = index into full_hourly_index for (init_times[i] + lead_time_vals[j])
    gather_idx = init_pos[:, None] + lead_pos[None, :]
    in_bounds = (gather_idx >= 0) & (gather_idx < len(full_hourly_index))
    safe_idx = np.clip(gather_idx, 0, len(full_hourly_index) - 1)

    site_datasets = []
    for site_id in hourly_ds.site_id.values:
        site_hourly = hourly_ds.sel(site_id=site_id)
        data_vars = {}
        for var in variables:
            values = site_hourly[var].values[safe_idx]
            values = np.where(in_bounds, values, np.nan)
            data_vars[var] = (("init_time", "lead_time"), values)
        site_ds = xr.Dataset(
            data_vars,
            coords={"init_time": init_times, "lead_time": lead_time_vals},
        )
        site_datasets.append(site_ds.expand_dims(site_id=[site_id]))

    ds = xr.concat(site_datasets, dim="site_id")
    # replicate the deterministic series across members for shape-compatibility with stage2 output
    ds = ds.expand_dims(ensemble_member=np.arange(n_members)).transpose(
        "init_time", "lead_time", "site_id", "ensemble_member"
    )
    return ds


if __name__ == "__main__":
    site_metadata_url = "https://raw.githubusercontent.com/eco4cast/usgsrc4cast-ci/main/USGS_site_metadata.csv"
    site_metadata = (
        pd.read_csv(site_metadata_url)
        .set_index('site_id')
        .to_xarray()
    )

    variables = ["downward_long_wave_radiation_flux_surface", "downward_short_wave_radiation_flux_surface",
                "maximum_temperature_2m", "minimum_temperature_2m", "precipitation_surface",
                "temperature_2m", "total_cloud_cover_atmosphere", "wind_u_10m", "wind_v_10m"]

    gefs_analysis = pull_gefs_analysis_flare(
        start_time=np.datetime64("2024-01-01"),
        end_time=np.datetime64("2024-01-10"),
        site_metadata=site_metadata,
        variables=variables,
    )
    print(gefs_analysis)

    gefs_operational = pull_gefs_operational_flare(
        start_time=np.datetime64("2024-01-01"),
        end_time=np.datetime64("2024-01-03"),
        site_metadata=site_metadata,
        lead_times="11d",
        variables=variables,
    )
    print(gefs_operational)
