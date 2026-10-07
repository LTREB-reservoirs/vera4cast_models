"""Convert BMI forecast output to the VERA4cast submission format.

The model predicts an asymmetric Laplace distribution (ALD) per site and lead
time, which isn't one of the VERA4cast parametric families, so each row is
expanded into an ``n_samples`` ensemble (``family = "ensemble"``) drawn from
evenly spaced ALD quantiles. When ``log_transformed`` is True the quantiles are
back-transformed with ``exp(q) - 0.01``, matching ``src/reliability_utils.R``.

Usage:
    python vera4cast_format.py out/forecast/<model_id>_2026-10-06_forecasts.parquet my_model_id
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from src.helper_utils import CSDMS_CHLA_ASYM, CSDMS_CHLA_LOC, CSDMS_CHLA_SCALE

VARIABLE = "Chla_ugL_mean"
# Depth (m) of the Chla_ugL_mean target at each VERA4cast site.
SITE_DEPTHS = {"fcre": 1.6, "bvre": 1.5}


def ald_quantiles(probs: np.ndarray, mu: np.ndarray, sigma: np.ndarray, p: np.ndarray) -> np.ndarray:
	"""Vectorized ALD quantile function (same parameterization as ``ald::qALD``).

	``probs`` has shape (n_samples,); ``mu``/``sigma``/``p`` have shape (n_rows,).
	Returns an array of shape (n_rows, n_samples).
	"""
	probs = probs[None, :]
	mu, sigma, p = mu[:, None], sigma[:, None], p[:, None]
	lower = mu + sigma * np.log(probs / p) / (1 - p)
	upper = mu - sigma * np.log((1 - probs) / (1 - p)) / p
	return np.where(probs < p, lower, upper)


def to_vera4cast(
	forecasts: pd.DataFrame,
	model_id: str,
	n_samples: int = 100,
	project_id: str = "vera4cast",
) -> pd.DataFrame:
	"""Expand ALD forecast parameters into a VERA4cast ``ensemble`` forecast.

	Parameters:
		forecasts: Output of ``bmi_lstm.forecast()``.
		model_id: Registered VERA4cast model_id.
		n_samples: Ensemble size per site and datetime.
		project_id: VERA4cast project_id.

	Returns:
		pd.DataFrame in the VERA4cast long format.
	"""
	unknown = set(forecasts["site_id"]) - SITE_DEPTHS.keys()
	if unknown:
		raise ValueError(f"No Chla_ugL_mean depth defined for site(s): {sorted(unknown)}")

	probs = (np.arange(n_samples) + 0.5) / n_samples
	q = ald_quantiles(
		probs,
		forecasts[CSDMS_CHLA_LOC].to_numpy(dtype=float),
		forecasts[CSDMS_CHLA_SCALE].to_numpy(dtype=float),
		forecasts[CSDMS_CHLA_ASYM].to_numpy(dtype=float),
	)
	log_rows = forecasts["log_transformed"].to_numpy(dtype=bool)
	q[log_rows] = np.exp(q[log_rows]) - 0.01
	# Chlorophyll-a can't be negative.
	q = np.clip(q, 0, None)

	out = pd.DataFrame({
		"project_id": project_id,
		"model_id": model_id,
		"datetime": np.repeat(pd.to_datetime(forecasts["datetime"]).to_numpy(), n_samples),
		"reference_datetime": np.repeat(pd.to_datetime(forecasts["reference_datetime"]).to_numpy(), n_samples),
		"duration": "P1D",
		"site_id": np.repeat(forecasts["site_id"].to_numpy(), n_samples),
		"depth_m": np.repeat(forecasts["site_id"].map(SITE_DEPTHS).to_numpy(), n_samples),
		"family": "ensemble",
		"parameter": np.tile(np.arange(1, n_samples + 1), len(forecasts)),
		"variable": VARIABLE,
		"prediction": q.ravel(),
	})
	for col in ("datetime", "reference_datetime"):
		out[col] = out[col].dt.tz_localize("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
	return out


def write_vera4cast(submission: pd.DataFrame, out_dir: str | Path) -> Path:
	"""Write ``submission`` as ``daily-{reference_date}-{model_id}.csv.gz``."""
	ref_date = pd.Timestamp(submission["reference_datetime"].iloc[0]).date()
	model_id = submission["model_id"].iloc[0]
	out_dir = Path(out_dir)
	out_dir.mkdir(parents=True, exist_ok=True)
	out_file = out_dir / f"daily-{ref_date}-{model_id}.csv.gz"
	submission.to_csv(out_file, index=False)
	print(f"Saved VERA4cast submission to {out_file}")
	return out_file


def main() -> None:
	parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
	parser.add_argument("forecast_file", help="forecast .csv or .parquet from forecast.py")
	parser.add_argument("model_id", help="registered VERA4cast model_id")
	parser.add_argument("--n-samples", type=int, default=100)
	parser.add_argument("--out-dir", default=None, help="default: next to forecast_file")
	args = parser.parse_args()

	path = Path(args.forecast_file)
	forecasts = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
	submission = to_vera4cast(forecasts, args.model_id, n_samples=args.n_samples)
	write_vera4cast(submission, args.out_dir or path.parent)


if __name__ == "__main__":
	main()
