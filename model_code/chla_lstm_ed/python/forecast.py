"""Generate chlorophyll-a forecasts from a trained model for one reference date.

``run_forecast(forecast_start)`` is the entry point used by the vera4cast_models
framework: it builds the encoder-decoder ``forecast_data`` file for
``forecast_start`` (see ``build_forecast_data.py``), runs the trained model,
converts the output to the VERA4cast submission format (see
``vera4cast_format.py``), and returns that submission DataFrame, whose
``reference_datetime`` is ``forecast_start``.

Usage (normally called from ../chla_lstm_workflow.py):
    python forecast.py                                     # config/model_config.yml, today
    python forecast.py config/model_config.yml 2026-10-06
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
from pathlib import Path

import pandas as pd

import build_forecast_data
from src.helper_utils import load_config
from src.torch_bmi import bmi_lstm
from vera4cast_format import to_vera4cast, write_vera4cast

# Model root (model_code/chla_lstm_ed/); config paths are relative to it.
MODEL_DIR = Path(__file__).resolve().parents[1]


def run_forecast(
	forecast_start: str | dt.date | None = None,
	config_file: str | Path = "config/model_config.yml",
	out_dir: str | Path | bool | None = None,
	build_data: bool = True,
	vera4cast_model_id: str | None = None,
) -> pd.DataFrame:
	"""Run the forecast with ``forecast_start`` as its reference date.

	Parameters:
		forecast_start: Forecast reference date (``"YYYY-MM-DD"`` or a date).
			Defaults to today.
		config_file: Model config. Relative paths resolve against the caller's
			working directory, then the model root.
		out_dir: Where to write the VERA4cast submission file; the raw ALD
			parameter CSV/parquet go in its ``ald_parameters/`` subdirectory.
			Defaults to the config's ``forecast_dir``. ``False`` skips writing files.
		build_data: Rebuild ``forecast_data_file`` for ``forecast_start`` before
			forecasting. Set False only if that file was already built for this date.
		vera4cast_model_id: Registered VERA4cast model_id for the submission.
			Defaults to the config's ``vera4cast_model_id``.

	Returns:
		pd.DataFrame: The forecast in VERA4cast submission format.
	"""
	ref_date = pd.Timestamp(forecast_start or dt.date.today()).normalize()

	config_file = Path(config_file)
	if not config_file.is_absolute():
		config_file = config_file.resolve() if config_file.exists() else MODEL_DIR / config_file
	if out_dir:
		out_dir = Path(out_dir).resolve()

	# Paths in the config are relative to the model root, so run from there
	# regardless of the caller's working directory.
	with contextlib.chdir(MODEL_DIR):
		if build_data:
			build_forecast_data.build(load_config(config_file), str(ref_date.date()))

		model = bmi_lstm()
		model.initialize(config_file=config_file, train=False)

		model_ref = pd.Timestamp(model.get_current_date()).normalize()
		if model_ref != ref_date:
			raise ValueError(
				f"forecast_data reference date {model_ref.date()} does not match "
				f"forecast_start {ref_date.date()}; rebuild it with build_data=True."
			)

		# Keep horizon semantics consistent with model type.
		f_horizon = int(model.cfg_bmi.get("f_horizon", 1))
		if model.model_type == "encoder_decoder":
			lead_time = f_horizon
		else:
			lead_time = max(f_horizon - 1, 0)

		forecasts = model.forecast(lead_time=lead_time)

		vera4cast_model_id = vera4cast_model_id or model.cfg_bmi.get("vera4cast_model_id")
		if not vera4cast_model_id:
			raise ValueError("Set vera4cast_model_id in the config or pass it to run_forecast().")
		submission = to_vera4cast(forecasts, vera4cast_model_id,
								  n_samples=int(model.cfg_bmi.get("n_samples", 100)))

		if out_dir is not False:
			out_dir = Path(out_dir or model.cfg_bmi.get("forecast_dir", "out/forecast/")).resolve()
			params_dir = out_dir / "ald_parameters"
			params_dir.mkdir(parents=True, exist_ok=True)
			stem = f"{model.model_id}_{ref_date.date()}_forecasts"
			out_csv = params_dir / f"{stem}.csv"
			out_parquet = params_dir / f"{stem}.parquet"

			forecasts.to_csv(out_csv, index=False)
			forecasts.to_parquet(out_parquet, index=False)
			print(f"Saved forecasts to {out_csv}")
			print(f"Saved forecasts to {out_parquet}")
			write_vera4cast(submission, out_dir)

	return submission


def main() -> None:
	parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
	parser.add_argument("config_file", nargs="?", default="config/model_config.yml")
	parser.add_argument("forecast_start", nargs="?", default=None,
						help="forecast reference date, e.g. 2026-10-06 (default: today)")
	parser.add_argument("--out-dir", default=None, help="override config['forecast_dir']")
	parser.add_argument("--no-build", action="store_true",
						help="reuse the existing forecast_data_file instead of rebuilding it")
	parser.add_argument("--vera4cast-model-id", default=None,
						help="override config['vera4cast_model_id']")
	args = parser.parse_args()

	run_forecast(args.forecast_start, config_file=args.config_file,
				 out_dir=args.out_dir, build_data=not args.no_build,
				 vera4cast_model_id=args.vera4cast_model_id)


if __name__ == "__main__":
	main()
