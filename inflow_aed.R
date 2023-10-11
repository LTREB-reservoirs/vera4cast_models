### Forecast nutrients
library(tidyverse)

inflow_targets_file <- 
  
met_target_file <- "https://s3.flare-forecast.org/targets/fcre_v2/fcre/observed-met_fcre.csv"

horizon <- 35
reference_datetime <- Sys.Date()  
ensemble_members <- 31
  
inflow_hist_dates <- tibble(datetime = seq(min(targets$datetime), max(targets$datetime), by = "1 day"))

filled_targets <- read_csv(inflow_targets_file, show_col_types = FALSE) |> 
  select(datetime, variable, observation) |> 
  pivot_wider(names_from = variable, values_from = observation) |> 
  right_join(inflow_hist_dates, by = "datetime") |> 
  mutate(across(flow_cms_mean:CH4_umolL_sample, imputeTS::na_interpolation)) |> 
  pivot_longer(-datetime, names_to = "variable", values_to = "observation")

forecast_datetimes <- seq(reference_datetime, length.out = horizon, by = "1 day")

variables <- unique(filled_targets_long$variable)

doy <- filled_targets_long |> 
  mutate(doy = lubridate::yday(datetime),
         log_observation = log(observation)) |> 
  summarize(mean = mean(log_observation),
            sd = 0.1,
            .by = c("doy", "variable"))

nutrient_forecast_df <- NULL

for(i in 1:length(forecast_datetimes)){
  for(k in 1:length(variables)){
    
    curr <- doy |> filter(doy == lubridate::yday(forecast_datetimes[i]),
                          variable == variables[k])
    
    samples <- rnorm(ensemble_members, mean = curr$mean, sd = curr$sd)
    samples <- exp(samples)
    current_df <- tibble(datetime = forecast_datetimes[i],
                         variable = variables[k],
                         prediction = samples,
                         parameter = 1:ensemble_members)
    
    forecast_df <- bind_rows(forecast_df, current_df)
  }
}

# Fore 
df_met <- readr::read_csv(met_target_file, show_col_types = FALSE)

df_met_precip <- df_met |> 
  filter(variable == "precipitation_flux" ) |> 
  mutate(observation = observation * 60 * 24) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  group_by(date) |> 
  summarise(precip = sum(observation, na.rm = TRUE)) |> 
  mutate(lag = lag(precip),
         tenday = zoo::rollsum(precip, k = 10, align = "right", fill = NA),
         fiveday = zoo::rollsum(precip, k = 5, align = "right", fill = NA),
         twentyday = zoo::rollsum(precip, k = 20, align = "right", fill = NA)) |> 
  rename(datetime = date)

df <- RopenMeteo::get_ensemble_forecast(
  latitude = 37.30,
  longitude = -79.83,
  forecast_days = horizon,
  past_days = 20,
  model = "gfs_seamless",
  variables = c("temperature_2m","precipitation"))

inflow_merged_precip <- df_inflow |> 
  filter(variable == "flow_cms_mean" & datetime > lubridate::as_date("2022-07-01") & datetime < lubridate::as_date("2023-06-01")) |> 
  left_join(df_met_precip, by = "datetime") |> 
  na.omit() |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))

forecast_met <- df |> 
  filter(variable == "precipitation" ) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(precip = sum(prediction, na.rm = TRUE), .by = c("date", "ensemble")) |> 
  group_by(ensemble) |> 
  mutate(fiveday = RcppRoll::roll_sum(precip, n = 5, fill = NA)) |> 
  rename(datetime = date) |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))

fit1 = lm(observation ~ month + fiveday, inflow_merged_precip)
summary(fit1)

flow_predicted <- forecast_met |> 
  modelr::add_predictions(fit1)

forecast_flow_df <- flow_predicted |> 
  select(datetime, ensemble, pred) |> 
  rename(predicted = pred) |> 
  mutate(variable = "flow_cms_mean") |> 
  na.omit() |> 
  arrange(datetime, ensemble)

####

df_met_temperature <- df_met |> 
  filter(variable == "air_temperature") |> 
  mutate(date = lubridate::as_date(datetime),
         observation = observation - 273.15) |> 
  summarise(temp = mean(observation, na.rm = TRUE), .by = "date") |> 
  mutate(lag = lag(temp),
         tenday = zoo::rollmean(temp, k = 10, align = "right", fill = NA),
         fiveday = zoo::rollmean(temp, k = 5, align = "right", fill = NA),
         twentyday = zoo::rollmean(temp, k = 20, align = "right", fill = NA)) |> 
  rename(datetime = date)

inflow_merged_temp <- df_inflow |> 
  filter(variable == "temp_c_mean") |> 
  left_join(df_met_temperature, by = "datetime") |> 
  na.omit() |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))

forecast_met <- df |> 
  filter(variable == "temperature_2m" ) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(temp = mean(prediction, na.rm = TRUE), .by = c("date", "ensemble")) |> 
  group_by(ensemble) |> 
  mutate(fiveday = RcppRoll::roll_mean(temp, n = 5, fill = NA)) |> 
  rename(datetime = date) |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))


fit1 = lm(observation ~ fiveday + month, inflow_merged_temp)
summary(fit1)

temp_predicted <-  forecast_met |> 
  modelr::add_predictions(fit1)

forecast_temp_df <- temp_predicted |> 
  select(datetime, ensemble, pred) |> 
  rename(predicted = pred) |> 
  mutate(variable = "temp_c_mean") |> 
  na.omit() |> 
  arrange(datetime, ensemble)

### combine and submit

forecast_df <- bind_rows(forecast_nutrient_df, forecast_flow_df, forecast_temp_df) |> 
  mutate(project = "vera4cast",
         model_id = "inflow_aed",
         family = "enemble",
         site_id = "tubr",
         duraiton = "P1D")

file_name <- paste0("inflow_aed-",Sys.Date(),"csv.gz")

vera4castHelpers::submit(file_name,first_submission = FALSE)







