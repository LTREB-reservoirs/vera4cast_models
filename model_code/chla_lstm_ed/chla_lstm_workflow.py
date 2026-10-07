# Workflow script
# Author: Austin Delany
# Date: 07Oct2026

# Purpose: run the chla encoder-decoder LSTM forecasting workflow for VERA, then
# rerun any forecasts missed in the past month.
#
# Usage (from the vera4cast_models root):
#   uv run --project model_code/chla_lstm_ed/environment \
#     python model_code/chla_lstm_ed/chla_lstm_workflow.py [YYYY-MM-DD] [--no-reruns]

import argparse
import datetime as dt
import sys
import traceback
from pathlib import Path

from pyarrow import fs

MODEL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(MODEL_DIR / "python"))

from forecast import run_forecast  # noqa: E402

challenge_model_name = "chla_lstm_ed"
config_file = MODEL_DIR / "config" / "model_config.yml"
out_dir = MODEL_DIR.parents[1] / "model_output" / challenge_model_name
lookback_days = 30


def submitted_dates() -> set[dt.date]:
    """Reference dates already in the VERA4cast forecast archive for this model."""
    s3 = fs.S3FileSystem(anonymous=True, endpoint_override="amnh1.osn.mghpcc.org", scheme="https")
    path = ("bio230121-bucket01/vera4cast/forecasts/archive-parquet/project_id=vera4cast/"
            f"duration=P1D/variable=Chla_ugL_mean/model_id={challenge_model_name}")
    dates = set()
    for info in s3.get_file_info(fs.FileSelector(path, allow_not_found=True)):
        try:
            dates.add(dt.date.fromisoformat(info.base_name.removeprefix("reference_date=")))
        except ValueError:
            continue
    return dates


def make_forecast(forecast_date: dt.date) -> bool:
    try:
        run_forecast(forecast_date, config_file=config_file, out_dir=out_dir,
                     vera4cast_model_id=challenge_model_name)
        return True
    except Exception:
        traceback.print_exc()
        print(f"FORECAST FAILED for {forecast_date}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("forecast_start", nargs="?", default=None,
                        help="forecast reference date, e.g. 2026-10-07 (default: today)")
    parser.add_argument("--no-reruns", action="store_true",
                        help="only make the forecast_start forecast; skip the missed-forecast check")
    args = parser.parse_args()

    today = dt.date.fromisoformat(args.forecast_start) if args.forecast_start else dt.date.today()
    failed = []

    print(f"==== Generating forecast for {today} ====")
    if not make_forecast(today):
        failed.append(today)

    if args.no_reruns:
        print("Reruns disabled; skipping missed-forecast check")
        report(failed)
        return

    # check for any missing forecasts
    print("==== Checking for missed forecasts ====")
    # Dates of forecasts: the past month, stopping two days back since recent
    # submissions may not have reached the archive yet
    this_month = [today - dt.timedelta(days=d) for d in range(lookback_days, 1, -1)]

    try:
        avail_dates = submitted_dates()
    except Exception:
        traceback.print_exc()
        print("Could not read the forecast archive; skipping reruns")
        avail_dates = set(this_month)

    rerun_dates = [d for d in this_month if d not in avail_dates]

    if rerun_dates:  ## CHECK IF THERE ARE MISSING DATES FOUND
        for i in rerun_dates:
            print(f"Remaking forecast for {i}")

            ## RERUN FORECAST HERE ##
            if not make_forecast(i):
                failed.append(i)
    else:
        print("NO MISSING FORECASTS FOUND")

    report(failed)


def report(failed: list[dt.date]) -> None:
    if failed:
        print(f"Forecasts failed for: {', '.join(str(d) for d in failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
