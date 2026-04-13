source('./R/secchi_mean3_kkh/secchi_forecast.R')


forecast_output <- secchi_forecast(forecast_start = Sys.Date(),
                                   se_period = 730,
                                   weeks = 4, 
                                   mean_obs = 3,
                                   sites = c("fcre", "bvre"),
                                   forecast_variable = "Secchi_m_sample",
                                   targets_url = "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz")

vera4castHelpers::forecast_output_validator(forecast_output)

vera4castHelpers::submit(forecast_output, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)
