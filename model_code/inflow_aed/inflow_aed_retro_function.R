inflow_aed_retro_function <- function(forecast_date){
  
  inflow_targets_file <- "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"
  
  met_target_file <- "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"
  
  horizon <- 34
  #reference_datetime <- lubridate::as_date("2024-09-30") 
  reference_datetime <- as.Date(forecast_date)
  noaa_date <- reference_datetime - lubridate::days(1)
  ensemble_members <- 31
  
  inflow_targets <- read_csv(inflow_targets_file, show_col_types = FALSE)
  
  inflow_hist_dates <- tibble(datetime = seq(min(inflow_targets$datetime), max(inflow_targets$datetime), by = "1 day"))
  
  filled_targets_long <- inflow_targets |> 
    filter(!variable %in% c("DN_mgL_sample", "DC_mgL_sample")) |> 
    select(datetime, variable, observation) |> 
    pivot_wider(names_from = variable, values_from = observation) |> 
    right_join(inflow_hist_dates, by = "datetime") |> 
    mutate(across(Flow_cms_mean:DIC_mgL_sample, imputeTS::na_interpolation)) |> 
    tidyr::fill(Flow_cms_mean:DIC_mgL_sample, .direction = "up") |>
    tidyr::fill(Flow_cms_mean:DIC_mgL_sample, .direction = "down") |> 
    pivot_longer(-datetime, names_to = "variable", values_to = "observation")
  
  filled_targets_long |> 
    ggplot(aes(x = datetime, y = observation)) +
    geom_line() +
    facet_wrap(~variable, scale = "free")
  
  forecast_datetimes <- seq(reference_datetime, length.out = horizon, by = "1 day")
  
  variables <- filled_targets_long |> 
    filter(!variable %in% c("Flow_cms_mean","Temp_C_mean")) |> 
    distinct(variable) |> 
    pull(variable)
  
  doy <- filled_targets_long |> 
    mutate(observation = ifelse(variable == "Temp_C_mean", observation + 273.15, observation), 
           observation = ifelse(observation == 0, 0.0001, observation),
           doy = lubridate::yday(datetime)) |> 
    summarize(mean = mean(observation),
              sd = sd(observation),
              .by = c("doy", "variable"))
  
  forecast_nutrient_df <- NULL
  
  for(i in 1:length(forecast_datetimes)){
    for(k in 1:length(variables)){
      
      curr <- doy |> filter(doy == lubridate::yday(forecast_datetimes[i]),
                            variable == variables[k])
      
      samples <- rnorm(ensemble_members, mean = curr$mean, sd = curr$sd)
      current_df <- tibble(datetime = forecast_datetimes[i],
                           variable = variables[k],
                           prediction = samples,
                           parameter = 1:ensemble_members) |> 
        mutate(prediction = ifelse(prediction < 0, 0, prediction))
      
      forecast_nutrient_df <- bind_rows(forecast_nutrient_df, current_df)
    }
  }
  
  forecast_nutrient_df <- forecast_nutrient_df |> 
    mutate(prediction = ifelse(variable == "Temp_C_mean", prediction - 273.15, prediction)) 
  
  # Forecast CMS and Temp 
  df_met <- readr::read_csv(met_target_file, show_col_types = FALSE)
  
  df_met_precip <- df_met |> 
    filter(variable == "Rain_mm_sum" ) |> 
    mutate(date = lubridate::as_date(datetime)) |> 
    group_by(date) |> 
    summarise(precip = sum(observation, na.rm = TRUE)) |> 
    mutate(lag = lag(precip),
           tenday = zoo::rollsum(precip, k = 10, align = "right", fill = NA),
           fiveday = zoo::rollsum(precip, k = 5, align = "right", fill = NA),
           twentyday = zoo::rollsum(precip, k = 20, align = "right", fill = NA)) |> 
    rename(datetime = date)
  
  
  met_s3_future <- arrow::s3_bucket(paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage2/reference_datetime=",noaa_date,"/site_id=fcre"),
                                    endpoint_override = "renc.osn.xsede.org",
                                    anonymous = TRUE)
  
  df_future <- arrow::open_dataset(met_s3_future) |> 
    select(datetime, parameter, variable, prediction) |> 
    filter(variable %in% c("precipitation_flux","air_temperature")) |> 
    collect() |> 
    rename(ensemble = parameter) |> 
    mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
           variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
           prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction))
  
  min_datetime <- min(df_future$datetime)
  
  met_s3_past <- arrow::s3_bucket(paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3/site_id=fcre"),
                                  endpoint_override = "renc.osn.xsede.org",
                                  anonymous = TRUE)
  
  past_date <- reference_datetime - lubridate::days(10)
  df_past <- arrow::open_dataset(met_s3_past) |> 
    select(datetime, parameter, variable, prediction) |> 
    filter(variable %in% c("precipitation_flux","air_temperature"),
           ((datetime <= min_datetime  & variable == "precipitation_flux") | 
              datetime < min_datetime  & variable == "air_temperature"),
           datetime > past_date) |> 
    collect() |> 
    rename(ensemble = parameter) |> 
    mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
           variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
           prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction))
  
  df <- bind_rows(df_future, df_past) |> 
    arrange(variable, datetime, ensemble)
  
  
  #df <- RopenMeteo::get_ensemble_forecast(
  #  latitude = 37.30,
  #  longitude = -79.83,
  #  forecast_days = horizon,
  #  past_days = 20,
  #  model = "gfs_seamless",
  #  variables = c("temperature_2m","precipitation"))
  
  inflow_merged_precip <- inflow_targets |> 
    filter(variable == "Flow_cms_mean" & datetime > lubridate::as_date("2022-07-01") & datetime < lubridate::as_date("2023-06-01")) |> 
    left_join(df_met_precip, by = "datetime") |> 
    filter(!is.na(observation)) |> 
    mutate(month = lubridate::month(datetime),
           season = ifelse(month > 4 & month < 11, "winter", "summer"),
           month = as.factor(month),
           season = as.factor(season))
  
  forecast_met <- df |> 
    filter(variable == "precipitation" ) |> 
    mutate(date = lubridate::as_date(datetime)) |> 
    summarise(precip = sum(prediction, na.rm = TRUE), .by = c("date", "ensemble")) |> 
    arrange(date, ensemble) |> 
    group_by(ensemble) |> 
    mutate(fiveday = RcppRoll::roll_sum(precip, n = 5, fill = NA,align = "right")) |> 
    rename(datetime = date) |> 
    mutate(month = lubridate::month(datetime),
           season = ifelse(month > 4 & month < 11, "winter", "summer"),
           #month = as.factor(month),
           season = as.factor(season))
  
  fit1 = lm(observation ~ precip + fiveday, inflow_merged_precip)
  summary(fit1)
  
  flow_predicted <- forecast_met |> 
    mutate(month = as.factor(month)) |> 
    filter(datetime >= reference_datetime) |> 
    modelr::add_predictions(fit1)
  
  forecast_flow_df <- flow_predicted |> 
    select(datetime, ensemble, pred) |> 
    rename(prediction = pred,
           parameter = ensemble) |> 
    mutate(parameter = as.numeric(parameter)) |> 
    mutate(variable = "Flow_cms_mean") |> 
    na.omit() |> 
    arrange(datetime, parameter) 
  
  
  ####
  
  df_met_temperature <- df_met |> 
    filter(variable == "AirTemp_C_mean") |> 
    mutate(date = lubridate::as_date(datetime)) |> 
    summarise(AirTemp_C_mean = mean(observation, na.rm = TRUE), .by = "date") |> 
    mutate(lag = lag(AirTemp_C_mean),
           tenday = zoo::rollmean(AirTemp_C_mean, k = 10, align = "right", fill = NA),
           fiveday = zoo::rollmean(AirTemp_C_mean, k = 5, align = "right", fill = NA),
           twentyday = zoo::rollmean(AirTemp_C_mean, k = 20, align = "right", fill = NA)) |> 
    rename(datetime = date)
  
  inflow_merged_temp <- inflow_targets |> 
    filter(variable == "Temp_C_mean") |> 
    left_join(df_met_temperature, by = "datetime") |> 
    filter(!is.na(observation)) |> 
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
  
  
  fit1 = lm(observation ~ fiveday, inflow_merged_temp)
  summary(fit1)
  
  temp_predicted <-  forecast_met |> 
    modelr::add_predictions(fit1)
  
  forecast_temp_df <- temp_predicted |> 
    select(datetime, ensemble, pred) |> 
    rename(prediction = pred,
           parameter = ensemble) |> 
    mutate(parameter = as.numeric(parameter)) |> 
    mutate(variable = "Temp_C_mean") |> 
    na.omit() |> 
    arrange(datetime, parameter)
  
  ### combine and submit
  
  forecast_df <- bind_rows(forecast_nutrient_df, forecast_flow_df, forecast_temp_df) |> 
    mutate(project_id = "vera4cast",
           model_id = "inflow_gefsClimAED",
           family = "ensemble",
           site_id = "tubr",
           duration = "P1D",
           depth_m = NA,
           datetime = lubridate::as_datetime(datetime),
           reference_datetime = lubridate::as_datetime(reference_datetime)) |> 
    filter(datetime >= reference_datetime)
  
  ggplot(forecast_df, aes(x = datetime, y = prediction, group= parameter)) + 
    geom_line() + facet_wrap(~variable, scale = "free")
  
  file_name <- paste0("inflow_gefsClimAED-",reference_datetime,".csv.gz")
  
  readr::write_csv(forecast_df, file_name)
  
  vera4castHelpers::submit(file_name,first_submission = FALSE)
  
  return(file_name)
}