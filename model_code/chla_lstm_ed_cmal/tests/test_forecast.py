# Test script
# Author: Austin Delany
# Date: 07Oct2026

# Purpose: run a single chla LSTM forecast (today by default) and save the
# VERA4cast-formatted output as a CSV for inspection. Does not check for missed
# forecasts and does not submit anything.
#
# Usage (from the vera4cast_models root):
#   uv run --project model_code/chla_lstm_ed_cmal/environment \
#     python model_code/chla_lstm_ed_cmal/tests/test_forecast.py [YYYY-MM-DD]

import datetime as dt
import sys
from pathlib import Path

MODEL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODEL_DIR / "python"))

from forecast import run_forecast  # noqa: E402

challenge_model_name = "chla_lstm_ed_cmal"
config_file = MODEL_DIR / "config" / "model_config.yml"
# Kept apart from the real submission files so test output is never submitted.
test_dir = MODEL_DIR.parents[1] / "model_output" / challenge_model_name / "test"


def main() -> None:
    forecast_date = dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else dt.date.today()
    print(f"==== Test forecast for {forecast_date} ====")

    forecast = run_forecast(forecast_date, config_file=config_file, out_dir=False,
                            vera4cast_model_id=challenge_model_name)

    test_dir.mkdir(parents=True, exist_ok=True)
    out_file = test_dir / f"daily-{forecast_date}-{challenge_model_name}.csv"
    forecast.to_csv(out_file, index=False)
    print(f"Saved test forecast to {out_file}")

    # Quick sanity checks
    print(f"Rows: {len(forecast)}; sites: {sorted(forecast['site_id'].unique())}; "
          f"datetimes: {forecast['datetime'].min()} .. {forecast['datetime'].max()}")
    n_missing = int(forecast["prediction"].isna().sum())
    if n_missing:
        print(f"WARNING: {n_missing} missing predictions")
    print(forecast.groupby("datetime")["prediction"].describe(percentiles=[0.05, 0.5, 0.95]).round(2))


if __name__ == "__main__":
    main()
