#### Function to run fdom AR model forecasts for VERA 
## DWH: July 2024

library(tidyverse)
library(arrow)

## helper functions
##function to pull current value 
current_value <- function(dataframe, variable, start_date){
  
  value <- dataframe |> 
    mutate(datetime = as.Date(datetime)) |> 
    filter(datetime == start_date,
           variable == variable) |> 
    pull(observation)
  
  return(value)
}

#function to generate 30 ensembles of fDOM IC based on standard deviation arround current observation
get_IC_uncert <- function(curr_fdom, n_members, ic_sd = 0.1){
  rnorm(n = n_members, mean = curr_fdom, sd = ic_sd)
}

## main function

generate_fDOM_forecast <- function(forecast_date, # a recommended argument so you can pass the date to the function
                                   forecast_horizon,
                                   n_members,
                                   output_folder,
                                   calibration_start_date,
                                   model_id,
                                   targets_url, # where are the targets you are forecasting?
                                   water_temp_4cast_old_url,
                                   #water_temp_4cast_new_url = 'focal',
                                   noaa_4cast_url,
                                   var, # what variable(s)?
                                   site, # what site(s),
                                   forecast_depths = 'focal',
                                   project_id = 'vera4cast') {
  
  # Put your forecast generating code in here, and add/remove arguments as needed.
  # Forecast date should not be hard coded
  # This is an example function that also grabs weather forecast information to be used as co-variates
  
  if (site == 'fcre' & forecast_depths == 'focal') {
    forecast_depths <- 1.6
  }
  
  if (site == 'bvre' & forecast_depths == 'focal') {
    forecast_depths <- 1.5
  }
  #-------------------------------------
  
  # Get targets
  message('Getting targets')
  targets <- readr::read_csv(targets_url, show_col_types = F) |>
    filter(variable %in% var,
           site_id %in% site,
           depth_m %in% forecast_depths,
           datetime <= forecast_date)
  #-------------------------------------
  
  # Get the weather data
  message('Getting weather')
  
  met_s3_future <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/drivers/met/gefs-v12/stage2",paste0("reference_datetime=",forecast_date),paste0("site_id=",site)),
                                   endpoint_override = 'renc.osn.xsede.org',
                                   anonymous = TRUE)
  
  forecast_weather <- arrow::open_dataset(met_s3_future) |> 
    dplyr::filter(
      #site_id == 'fcre',
      variable %in% c("precipitation_flux", "surface_downwelling_shortwave_flux_in_air")) |>
    mutate(datetime_date = as.Date(datetime),
           reference_datetime = forecast_date) |>
    group_by(reference_datetime, datetime_date, variable, parameter) |>
    summarise(prediction = mean(prediction, na.rm = T), .groups = "drop") |>
    mutate(parameter = parameter + 1) |> 
    dplyr::collect()  
  
  # split it into historic and future
  
  historic_noaa_s3 <- arrow::s3_bucket(paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3/site_id=",site),
                                       endpoint_override = "renc.osn.xsede.org",
                                       anonymous = TRUE)
  
  historic_weather <- arrow::open_dataset(historic_noaa_s3) |> 
    filter(datetime < forecast_date,
           variable %in% c("precipitation_flux", "surface_downwelling_shortwave_flux_in_air")) |> 
    collect() |> 
    #filter(as.Date(reference_datetime) == as.Date(datetime)) |> #get data from just day of forecast issued
    group_by(datetime, variable) |> 
    summarise(prediction = mean(prediction, na.rm = T), .groups = "drop") |>  #get daily means (from ensembles) for each variable
    pivot_wider(names_from = variable, values_from = prediction)
    #filter(ymd(datetime) < forecast_date) |> 
    # mutate(reference_datetime = as.Date(reference_datetime)) |> 
    # rename(datetime = reference_datetime)
  
  # historic_weather <- noaa_4cast |> 
  #   filter(reference_datetime == datetime_date) |> #get data from just day of forecast issued
  #   group_by(reference_datetime, variable) |> 
  #   summarise(prediction = mean(prediction, na.rm = T), .groups = "drop") |>  #get daily means (from ensembles) for each variable
  #   pivot_wider(names_from = variable, values_from = prediction) |> 
  #   filter(ymd(reference_datetime) < forecast_date) |> 
  #   mutate(reference_datetime = as.Date(reference_datetime)) |> 
  #   rename(datetime = reference_datetime)
  
  
  # forecast_weather <- noaa_4cast |>
  #   filter(ymd(reference_datetime) == forecast_date) 
  # 
  
  #-------------------------------------
  
  #Get water temp forecasts
  message('Getting water temp 4casts')
  
  
  ### old water temp 
  backup_forecasts <- arrow::s3_bucket(paste0(water_temp_4cast_old_url,'site_id=',site,'/model_id=test_runS3/'),
                                       endpoint_override = 'renc.osn.xsede.org',
                                       anonymous = TRUE)
  
  df_flare_old <- arrow::open_dataset(backup_forecasts) |>
    filter(depth %in% c(1.5), #no 1.6
           #site_id %in% c("bvre", "fcre"),
           variable == "temperature",
           #model_id == "test_runS3", #other models for FCR, this is the only one for BVR in backups bucket
           parameter <= 31,
           reference_datetime > "2022-11-07 00:00:00") |>
    dplyr::collect()
  
  df_flare_old_forbind <- df_flare_old |> 
    rename(datetime_date = datetime) |> 
    # filter(parameter <= 31,
    #        site_id == site) |> 
    mutate(site_id = site,
           model_id = 'test_runs3') |> 
    filter(as.Date(reference_datetime) > ymd("2022-11-07") ) |>  #remove odd date that has dates one month behind reference datetime
    select(reference_datetime, datetime_date, site_id, depth, family, parameter, variable, prediction, model_id)
  
  
  ## current water temp 
  
  if (site == 'fcre'){
    
    #water_temp_4cast_new_url <- "s3://anonymous@bio230121-bucket01/vera4cast/forecasts/parquet/project_id=vera4cast/duration=P1D/variable=Temp_C_mean?endpoint_override=renc.osn.xsede.org"
    fcre_reforecast <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/forecasts/parquet/site_id=fcre/model_id=glm_flare_v1/"),
                                        endpoint_override = 'renc.osn.xsede.org',
                                        anonymous = TRUE)
    
    #new_flare_forecasts <- arrow::open_dataset(fcre_reforecast)
    
    df_flare_new <- arrow::open_dataset(fcre_reforecast) |>
      dplyr::filter(depth == 1.5, 
                    variable == 'temperature') |> 
        # site_id %in% c("fcre"),
        #             model_id == "glm_flare_v1",
        #             depth_m == 1.5) |>
      #dplyr::rename(depth = depth_m) |> 
      dplyr::collect()
    
  } else if (site == 'bvre') {
    bvre_reforecast <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/forecasts/parquet/site_id=bvre/model_id=glm_flare_v1/"),
                                        endpoint_override = 'renc.osn.xsede.org',
                                        anonymous = TRUE)
    
    df_flare_new <- arrow::open_dataset(bvre_reforecast) |> 
      filter(variable == 'temperature',
             # site_id == 'bvre', 
             # model_id == 'glm_flare_v1',
             depth == 1.5) |> 
      collect()
    
  } else{
    message('Site Error: Please use "fcre" or "bvre" as the site identifier')
    stop()
  }
  
  
  df_flare_new_forbind <- df_flare_new |> 
    filter(reference_datetime > ymd_hms("2024-02-18 00:00:00")) |> 
    select(-reference_date) |> 
    mutate(variable = "temperature",
           reference_datetime = as.character(reference_datetime),
           site_id = site,
           model_id = 'test_runs3') |> 
    rename(datetime_date = datetime) |> 
    mutate(parameter = as.numeric(parameter)) |> 
    select(reference_datetime, datetime_date, site_id, depth, family, parameter, variable, prediction, model_id)
  
  
  
  ## bind water temp forecasts together 
  
  water_temp_4cast_data <- rbind(df_flare_old_forbind, df_flare_new_forbind) |>
    filter(parameter <= 31)
  
  
  # split it into historic and future
  historic_watertemp <- water_temp_4cast_data |>
    filter(as.Date(datetime_date) == as.Date(reference_datetime)) |> 
    # calculate a daily mean (remove ensemble)
    group_by(reference_datetime, variable) |>
    summarise(prediction = mean(prediction, na.rm = T), .groups = "drop") |>
    pivot_wider(names_from = variable, values_from = prediction) |> 
    filter(as.Date(reference_datetime) < forecast_date
    ) |> 
    mutate(reference_datetime = as.Date(reference_datetime)) |> 
    rename(datetime = reference_datetime)
  
  
  forecast_watertemp <- water_temp_4cast_data |>
    filter(as.Date(reference_datetime) == forecast_date) 
  
  
  #-------------------------------------
  
  
  
  # Fit model
  message('Fitting model')
  
  fit_df <- targets |>
    filter(datetime < forecast_date,
           datetime >= calibration_start_date ## THIS is the furthest date that we have all values for calibration
    ) |>
    pivot_wider(names_from = variable, values_from = observation) |>
    left_join(historic_weather) |>
    left_join(historic_watertemp) |>
    mutate(fDOM_lag1 = lag(fDOM_QSU_mean, 1),
           precip_lag1 = lag(precipitation_flux, 1))
  
  fdom_model <- lm(fit_df$fDOM_QSU_mean ~ fit_df$fDOM_lag1 + fit_df$surface_downwelling_shortwave_flux_in_air +
                     fit_df$precipitation_flux + fit_df$precip_lag1 + fit_df$temperature)
  
  model_fit <- summary(fdom_model)
  
  coeffs <- model_fit$coefficients[,1]
  params_se <- model_fit$coefficients[,2] 
  
  # #### get param uncertainty
  #get param distribtuions for parameter uncertainity
  param_df <- data.frame(beta_int = rnorm(31, coeffs[1], params_se[1]),
                         beta_fdomLag = rnorm(31, coeffs[2], params_se[2]),
                         beta_SW = rnorm(31, coeffs[3], params_se[3]),
                         beta_rain = rnorm(31, coeffs[4], params_se[4]),
                         beta_rainLag = rnorm(31, coeffs[5], params_se[5]),
                         beta_temp = rnorm(31, coeffs[6], params_se[6])
  )
  
  
  
  ####get process uncertainty
  #find residuals
  fit_df_noNA <- na.omit(fit_df)
  mod <- predict(fdom_model, data = fit_df_noNA)
  residuals <- mod - fit_df_noNA$fDOM_QSU_mean
  sigma <- sd(residuals, na.rm = TRUE) # Process Uncertainty Noise Std Dev.; this is your sigma
  
  
  ####look at set up for IC uncert 
  ic_sd <- 0.1 #adpating from HLWs temp 4cast using 0.1 and detection limit on fDOM sensor being 0.07 QSU
  # ic_uc <- rnorm(n = 30, mean = mean(fcr_fdom_2023$observation, na.rm = T), sd = ic_sd)
  # hist(ic_uc)
  
  # param_df$sigma <- sigma
  # param_df$ic_sd <- ic_sd
  
  # return(param_df)
  
  #-------------------------------------
  
  # Set up forecast data frame
  
  message('Make forecast dataframe')
  
  #establish forecasted dates
  forecasted_dates <- seq(from = ymd(forecast_date), to = ymd(forecast_date) + forecast_horizon, by = "day")
  
  #get current fdom value
  curr_fdom <- current_value(dataframe = targets,variable = var, start_date = forecast_date)
  
  #set up df of different initial conditions for IC uncert
  ic_df <- tibble(date = rep(as.Date(forecast_date), times = n_members),
                  ensemble_member = c(1:n_members),
                  forecast_variable = var,
                  value = get_IC_uncert(curr_fdom, n_members, ic_sd = 0.1),
                  uc_type = "total")
  
  #set up table to hold forecast output 
  forecast_full_unc <- tibble(date = rep(forecasted_dates, times = n_members),
                              ensemble_member = rep(1:n_members, each = length(forecasted_dates)),
                              reference_datetime = forecast_date,
                              Horizon = date - reference_datetime,
                              forecast_variable = var,
                              value = as.double(NA),
                              uc_type = "total") |> 
    rows_update(ic_df, by = c("date","ensemble_member","forecast_variable", "uc_type")) # adding IC uncert
  
  
  #-------------------------------------
  
  message('Generating forecast')
  
  
  #for loop to run forecast 
  for(i in 2:length(forecasted_dates)) {
    
    #pull prediction dataframe for the relevant date
    fdom_pred <- forecast_full_unc %>%
      filter(date == forecasted_dates[i])
    
    #pull driver ensemble for the relevant date; here we are using all 31 NOAA ensemble members
    met_sw_driv <- forecast_weather %>%
      filter(variable == "surface_downwelling_shortwave_flux_in_air") |> 
      filter(ymd(reference_datetime) == forecast_date) |> 
      filter(ymd(datetime_date) == forecasted_dates[i])
    
    met_precip_driv <- forecast_weather %>%
      filter(variable == "precipitation_flux") |> 
      filter(ymd(reference_datetime) == forecast_date) |> 
      filter(ymd(datetime_date) == forecasted_dates[i])
    
    met_precip_lag_driv <- forecast_weather %>%
      filter(variable == "precipitation_flux") |> 
      filter(ymd(reference_datetime) == forecast_date) |> 
      filter(ymd(datetime_date) == forecasted_dates[i-1])
    
    flare_driv <- forecast_watertemp %>%
      filter(as.Date(reference_datetime) == forecast_date) |> 
      filter(as.Date(datetime_date) == forecasted_dates[i]) #|>
      #slice(1:31)
    
    #pull lagged fdom values
    fdom_lag <- forecast_full_unc %>%
      filter(date == forecasted_dates[i-1])
    
    #run model
    fdom_pred$value <- param_df$beta_int + (fdom_lag$value * param_df$beta_fdomLag)  +
      (met_sw_driv$prediction * param_df$beta_SW) + (met_precip_driv$prediction * param_df$beta_rain) + 
      (met_precip_lag_driv$prediction * param_df$beta_rainLag) + (flare_driv$prediction * param_df$beta_temp) +
      rnorm(n = 31, mean = 0, sd = sigma) #process uncert
    
    #insert values back into the forecast dataframe
    forecast_full_unc <- forecast_full_unc %>%
      rows_update(fdom_pred, by = c("date","ensemble_member","forecast_variable","uc_type"))
    
  } #end for loop
  
  #clean up file to match vera format 
  
  forecast_df <- forecast_full_unc |>
    rename(datetime = date,
           variable = forecast_variable,
           prediction = value,
           parameter = ensemble_member) |>
    mutate(family = 'ensemble',
           duration = "P1D",
           depth_m = forecast_depths,
           project_id = project_id,
           model_id = model_id,
           site_id = site
    ) |>
    select(datetime, reference_datetime, model_id, site_id,
           parameter, family, prediction, variable, depth_m,
           duration, project_id)
  
  return(forecast_df)
  #return(write.csv(forecast_df, file = output_folder, row.names = F))
  # return(write.csv(forecast_df, file = paste0("C:/Users/dwh18/OneDrive/Desktop/R_Projects/fDOM_forecasting/Data/ASLO_talk_forecast_output/", output_folder, "/forecast_full_unc_", forecast_date, '.csv'), row.names = F))
  
  
}  ##### end function


# ########### Test function #######
# 
# #### set function inputs
# ## CHANGE FIRST TWO
# forecast_date <- ymd("2024-07-01")
# site <- "bvre"
# forecast_depths <- 'focal'
# 
# forecast_horizon <- 16
# n_members <- 31
# calibration_start_date <- ymd("2022-11-11")
# model_id <- "fDOM_AR_dwh"
# targets_url <- "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"
# 
# water_temp_4cast_old_url <- "bio230121-bucket01/vt_backup/forecasts/parquet/"
# 
# #water_temp_4cast_new_url <- 'focal'
# 
# noaa_4cast_url <- "bio230121-bucket01/flare/drivers/met/gefs-v12/stage2"
# 
# var <- "fDOM_QSU_mean"
# project_id <- "vera4cast"
# 
# output_folder <- paste0("C:/Users/dwh18/Downloads/", model_id, "_", site, "_", forecast_date, ".csv")

# 
# 
# 
# ##run function
# generate_fDOM_forecast(forecast_date = forecast_date, forecast_horizon = forecast_horizon, n_members = n_members,
#                        output_folder = output_folder, model_id = model_id, targets_url = targets_url,
#                        water_temp_4cast_old_url = water_temp_4cast_old_url,
#                        # water_temp_4cast_new_url = water_temp_4cast_new_url,
#                        noaa_4cast_url = noaa_4cast_url, var = var,
#                        site = site, forecast_depths = forecast_depths, project_id = project_id, 
#                        calibration_start_date = calibration_start_date )
# 
# 
# read.csv("C:/Users/dwh18/Downloads/fDOM_AR_dwh_fcre_2024-07-01.csv")|>
#   mutate(date = as.Date(datetime)) |>
#   # filter(forecast_date > ymd("2023-01-03")) |>
#   ggplot(aes(x = date, y = prediction, color = as.character(parameter)))+
#   geom_line()









