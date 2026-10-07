from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.helper_utils import load_config


def _get_site_ids(site_metadata: xr.Dataset | pd.DataFrame) -> list[str]:
    if isinstance(site_metadata, xr.Dataset):
        if "site_id" in site_metadata.coords:
            return [str(v) for v in site_metadata.coords["site_id"].values]
        return [str(v) for v in site_metadata.to_dataframe().index.unique().tolist()]
    return [str(v) for v in site_metadata["site_id"].astype(str).tolist()]


def pull_static_features(site_metadata: xr.Dataset | pd.DataFrame, variables: list[str],
                         data_in_dir: str | None = None) -> xr.Dataset:
    """Return requested static variables as an xarray Dataset indexed by site_id."""
    if data_in_dir is None:
        config = load_config("model_config.yml")
        data_in_dir = config.get("data_in_dir", "in/")
    static_file = Path(data_in_dir) / "static_features" / "static_features.csv"

    if not static_file.exists():
        raise FileNotFoundError(
            f"Static feature file not found: {static_file}. "
            "Run get_static_features.py first."
        )

    df = pd.read_csv(static_file)
    site_col = "site_id" if "site_id" in df.columns else None
    if site_col is None:
        raise ValueError(f"Expected a site_id column in {static_file}; found {list(df.columns)}")

    df[site_col] = df[site_col].astype(str).str.strip()
    for var in variables:
        if var not in df.columns:
            df[var] = np.nan

    site_ids = _get_site_ids(site_metadata)
    lookup = df.set_index(site_col).reindex(site_ids)

    data_vars = {
        var: (("site_id",), pd.to_numeric(lookup[var], errors="coerce").to_numpy()) for var in variables
    }

    # Keep source_dataset so downstream filtering by requested site groups works.
    if isinstance(site_metadata, xr.Dataset) and "source_dataset" in site_metadata.data_vars:
        source_lookup = site_metadata["source_dataset"].to_series().reindex(site_ids)
        data_vars["source_dataset"] = (("site_id",), source_lookup.astype(str).to_numpy())
    elif isinstance(site_metadata, pd.DataFrame) and "source_dataset" in site_metadata.columns:
        source_lookup = site_metadata.set_index("site_id")["source_dataset"].reindex(site_ids)
        data_vars["source_dataset"] = (("site_id",), source_lookup.astype(str).to_numpy())

    return xr.Dataset(data_vars=data_vars, coords={"site_id": site_ids})
