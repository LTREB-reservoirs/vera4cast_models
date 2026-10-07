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


def _normalize_site_id(series: pd.Series, prefix: str = "") -> pd.Series:
    out = series.astype(str).str.strip()
    if prefix:
        numeric_mask = out.str.match(r"^\d+$")
        out.loc[numeric_mask] = prefix + out.loc[numeric_mask]
    return out


def _read_site_table(path_or_url: str, source_dataset: str, *, usgs_prefix: bool = False) -> pd.DataFrame:
    df = pd.read_csv(path_or_url)
    df.columns = df.columns.str.strip()  # some sources (e.g. VERA4cast) pad headers with spaces
    site_col = _pick_column(df, ["site_id", "site", "site_no", "location_id", "station_id"])
    lat_col = _pick_column(df, ["latitude", "lat", "dec_lat_va"])
    lon_col = _pick_column(df, ["longitude", "lon", "long", "dec_long_va"])

    if site_col is None or lat_col is None or lon_col is None:
        raise ValueError(
            f"Could not identify site_id/latitude/longitude columns in {path_or_url}. "
            f"Found columns: {list(df.columns)}"
        )

    out = pd.DataFrame(
        {
            "site_id": _normalize_site_id(df[site_col], prefix="USGS-" if usgs_prefix else ""),
            "latitude": pd.to_numeric(df[lat_col], errors="coerce"),
            "longitude": pd.to_numeric(df[lon_col], errors="coerce"),
            "source_dataset": source_dataset,
        }
    ).dropna(subset=["site_id", "latitude", "longitude"])
    return out.drop_duplicates(subset=["site_id"], keep="first")


def get_metadata(
    site_metadata_url: str,
    savoy_metadata_file: str | None = None,
    stackpoole_metadata_file: str | None = None,
    sites: list[str] | None = None,
) -> xr.Dataset:
    """Load and harmonize site metadata for requested source datasets."""
    sites = sites or ["usgsrc4cast"]
    frames: list[pd.DataFrame] = []

    if "usgsrc4cast" in sites:
        frames.append(_read_site_table(site_metadata_url, "usgsrc4cast", usgs_prefix=True))

    if "vera4cast" in sites:
        frames.append(_read_site_table(site_metadata_url, "vera4cast", usgs_prefix=False))

    if "savoy" in sites and savoy_metadata_file and Path(savoy_metadata_file).exists():
        frames.append(_read_site_table(savoy_metadata_file, "savoy"))

    if "stackpoole" in sites and stackpoole_metadata_file and Path(stackpoole_metadata_file).exists():
        frames.append(_read_site_table(stackpoole_metadata_file, "stackpoole"))

    if not frames:
        raise FileNotFoundError(
            "No metadata sources were found for requested sites. "
            "Check site metadata URL/path values in model_config.yml."
        )

    meta = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["site_id"], keep="first")
    meta = meta.sort_values("site_id").set_index("site_id")
    return xr.Dataset.from_dataframe(meta)
