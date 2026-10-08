# chla_lstm_ed — training results

The daily `chla_lstm_ed` forecast uses **`20261008_ed_chlalast_vera_CMAL_h16_e90d34`**,
trained 2026-10-08 in `LaplaceDistributionChlaForecast` and copied here. The settings
it needs at inference are in `../config/model_config.yml`.

## Model

Encoder-decoder LSTM with a CMAL (asymmetric Laplace) output layer. It forecasts
`Chla_ugL_mean` at fcre, 1.6 m, for 34 days (day 0 = reference date). Each day's
distribution is converted to a 100-member ensemble for submission.

- **Encoder:** the 90 days before the reference date. 21 inputs: FLARE stage3 weather
  (with cloud cover from the dynamical.org GEFS analysis), Tunnel Branch inflow, 9
  static site features, and observed chlorophyll lagged 1 day (gaps carried forward
  up to 7 days, never using later data).
- **Decoder:** 34 forecast days. 28 inputs: the stage2 GEFS ensemble median and
  90% spread (PI90) for the 9 weather variables (cloud cover from the dynamical.org
  GEFS 35-day forecast), the static features, and `chla_last_obs`, the last
  observation available at forecast time (R−2) repeated on every day.
- **Weather lag:** each window uses the GEFS forecast issued the day before the
  reference date, in training as in operation.
- **Size:** 16 hidden units, input dropout 0.3.

## Training

| Split | Start dates | Windows |
|---|---|---|
| Train | 2020-12-30 to 2022-11-28, and 2025-01-01 to 2025-11-28 | 1,030 |
| Validation (early stopping) | 2024-01-01 to 2024-11-28 | 333 |
| Test | 2023-01-01 to 2023-11-28 | 332 |

All 34 target days of every window fall inside its own split's year(s), so no
target day is shared between splits. Training stopped early after 59 epochs. The
saved weights are from epoch 7, which had the best validation loss.

11 training windows (2025-07-26 to 2025-08-05) use day-of-year climatology for the
forecast weather, because those stage2 forecasts are missing from FLARE's archive.

## 2023 test scores

CRPS in µg/L (lower is better), scored against fcre observations. The full table,
including the median's mean absolute error and the number of scored days, is in
`model_train/20261008_ed_chlalast_vera_CMAL_h16_e90d34_test2023_scores.csv`.

| Lead days | This model | Climatology | Persistence |
|---|---|---|---|
| 0 | 3.72 | 6.99 | 2.56 |
| 1–3 | 3.99 | 7.02 | 3.74 |
| 4–10 | 4.77 | 7.14 | 6.01 |
| 11–20 | 5.43 | 7.35 | 8.47 |
| 21–33 | 6.23 | 7.85 | 10.71 |

- **Climatology:** observed chlorophyll from the training years within ±7 days of
  the same day of year.
- **Persistence:** the last observation available at forecast time (R−2), used for
  every lead day. It's a single value, so its CRPS is its absolute error.

The model beats climatology at every lead time and persistence from about day 4
onward. Persistence is still better at day 0 and slightly better at days 1–3.

Two other candidates trained the same day scored about 5.8–6.8 at every lead time.
They had 64 hidden units and no dropout; one used no chlorophyll at all, and the
other used chlorophyll history without `chla_last_obs`. Both are in the CSV.

## Caveats

- **Early stopping:** validation loss is lowest at epoch 7, so the model is still
  data-limited (one site, about 1,000 training windows).
- **Earlier scores were inflated:** models trained before 2026-10-08 (including
  `20261005_ed_noAR_vera_CMAL_h64_e90d11`) had training windows overlapping their
  validation and test years, so their reported scores are optimistic and not
  comparable to these.

## Files

- `model_train/20261008_ed_chlalast_vera_CMAL_h16_e90d34_wgts/weights.pth`: trained weights (used daily).
- `training_data/20261008_ed_chlalast_vera_CMAL_h16_e90d34.npz`: input scaling values and variable order (used daily), plus the training arrays.
- `model_train/20261008_ed_chlalast_vera_CMAL_h16_e90d34_{train_log.csv,config.yml,*_preds.feather}`: training log, the full training config, and train/validation/test predictions (reference only).
- `static_features/static_features.csv`: site features (used daily).
- `*20261005_ed_noAR_vera_CMAL_h64_e90d11*`: the previously deployed model, kept for rollback. It needs its own settings (11-day horizon, 64 hidden units, no chlorophyll inputs).
