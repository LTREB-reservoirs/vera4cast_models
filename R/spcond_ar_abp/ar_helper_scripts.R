#### Function to run sp.cond AR model forecasts for VERA
#### Heavily borrowed from BWH fdom AR model
## ABP: Feb. 2026

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
# get_IC_uncert <- function(current_value, n_members, ic_sd = 0.05){
#   rnorm(n = n_members, mean = current_value, sd = ic_sd)
# }

## main function

generate_spcond_forecast <- function(forecast_date, # a recommended argument so you can pass the date to the function
                                   forecast_horizon,
                                   n_members,
                                   output_folder,
                                   calibration_start_date,
                                   model_id,
                                   targets_url, # where are the targets you are forecasting?
                                   inflow_4cast_url,
                                   var, # what variable(s)?
                                   site, # what site(s),
                                   forecast_depths = 'focal',
                                   project_id = 'vera4cast') {
  
  # # forecast_date <- as.Date("2025-07-01") #- lubridate::days(3)
  # forecast_date <- Sys.Date() - lubridate::days(1)
  # site <- "fcre"
  # forecast_depths <- 'focal'
  # 
  # forecast_horizon <- 34
  # n_members <- 31
  # calibration_start_date <- ymd("2019-01-01")
  # model_id <- "spcond_AR_abp"
  # targets_url <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"
  # inflow_4cast_url <- 'bio230121-bucket01/vera4cast/forecasts/archive-parquet/project_id=vera4cast/duration=P1D'
  # 
  # var <- "SpCond_uScm_mean"
  # project_id <- "vera4cast"

  
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
  targets <- readr::read_csv(targets_url, show_col_types = F)|>
    filter(variable %in% var,
           site_id %in% site,
           depth_m %in% forecast_depths,
           datetime <= forecast_date)
  
  
  
  #-------------------------------------
  
  #Get inflow forecasts
  message('Getting inflow 4casts')
  
 # Here is the code for accessing FCR inflow forecasts. The way it is set up will take some time to complete, but you can add additional filters (variable, reference_datetime, etc.) which will speed things up
 
  
  
  ### infow forecasts
  # bundled can go faster but not working for me. 
  s3_path <- arrow::s3_bucket(bucket = inflow_4cast_url, 
                              endpoint_override = 'https://amnh1.osn.mghpcc.org', 
                              anonymous = TRUE)
  
  df_flare_old <- arrow::open_dataset(s3_path) |>
    filter(model_id == 'inflow_gefsClimAED',
           variable %in% c('Flow_cms_mean'),
          # parameter < 32, # parameter is a character and skips 4,5,6,7,8,9
           reference_datetime > "2019-01-01 00:00:00") |>
    collect()
  
  
  df_inflow_forecast <- df_flare_old |> 
    rename(datetime_date = datetime) |> 
    # filter(parameter <= 31,
    #        site_id == site) |> 
    mutate(site_id = site)|>
          # model_id = 'test_runs3') |> 
    filter(as.Date(reference_datetime) > ymd("2022-11-07") ) |>  #remove odd date that has dates one month behind reference datetime
    select(reference_datetime, datetime_date, site_id, family, parameter, variable, prediction, model_id)|>
    # convert the Flow to residence time and then take the log of that 
    mutate(WRT_days_forecast = ( (3.1E5/prediction) * (1/60) * (1/60) * (1/24) ),
          log_WRT_days_forecast = log(WRT_days_forecast))
  
  
  
  
  # split it into historic and future
  historic_inflow <- df_inflow_forecast |>
    filter(as.Date(datetime_date) == as.Date(reference_datetime)) |> 
    # calculate a daily mean (remove ensemble)
    group_by(reference_datetime, variable) |>
    summarise(prediction = mean(log_WRT_days_forecast, na.rm = T), .groups = "drop") |>
    pivot_wider(names_from = variable, values_from = prediction) |> 
    filter(as.Date(reference_datetime) < forecast_date
    ) |> 
    mutate(reference_datetime = as.Date(reference_datetime)) |> 
    rename(datetime = reference_datetime)
  
  
  forecast_inflow <- df_inflow_forecast |>
    filter(as.Date(reference_datetime) == forecast_date)
  
  if (nrow(forecast_inflow) == 0){
    message(paste0('Inflow forecast for ', forecast_date, ' is not available...stopping model'))
    stop()
  }
  
  
  #-------------------------------------
  
  
  
  # Fit model
  message('Fitting model')
  
  fit_df <- targets |>
    filter(datetime < forecast_date,
           datetime >= calibration_start_date ## THIS is the furthest date that we have all values for calibration
    ) |>
    pivot_wider(names_from = variable, values_from = observation) |>
    # add in the historical WRT that is log transformed. Better Rsquared
    left_join(historic_inflow) |>
    # get a one-day log, two-day lag, and three-day lag
    mutate(spcond_lag1 = lag(SpCond_uScm_mean, 1),
           spcond_lag2 = lag(SpCond_uScm_mean, 2),
           spcond_lag3 = lag(SpCond_uScm_mean, 3))|>
    drop_na(Flow_cms_mean)
  
  spcond_model <- lm(SpCond_uScm_mean ~ Flow_cms_mean + spcond_lag1 + spcond_lag2 + spcond_lag3, data = fit_df)
  
  model_fit <- summary(spcond_model)
  
  coeffs <- model_fit$coefficients[,1]
  params_se <- model_fit$coefficients[,2] 
  
  # #### get param uncertainty
  #get param distribtuions for parameter uncertainity. Not using parameter uncertainity right now.
  param_df <- data.frame(beta_int = rnorm(n_members, coeffs[1], params_se[1]),
                         beta_WRT = rnorm(n_members, coeffs[2], params_se[2]),
                         beta_sp_lag1 = rnorm(n_members, coeffs[3], params_se[3]),
                         beta_sp_lag2 = rnorm(n_members, coeffs[4], params_se[4]),
                         beta_sp_lag3 = rnorm(n_members, coeffs[5], params_se[5]))

  
  
  ####get process uncertainty
  #find residuals. Not using the process uncertainty right now
  fit_df_noNA <- na.omit(fit_df) # take out any NAs from the targets file
  fit_df_noNA$mod <- predict(spcond_model, data = fit_df_noNA)
  residuals <- fit_df_noNA$mod - fit_df_noNA$SpCond_uScm_mean
  sigma <- sd(residuals, na.rm = TRUE) # Process Uncertainty Noise Std Dev.; this is your sigma
  
  
  ####look at set up for IC uncert 
  ic_sd <- 0.1 #adpating from HLWs temp 4cast using 0.1 and detection limit on fDOM sensor being 0.07 QSU, 2 uS/cm is the accuracy from the EXO manual
   ic_uc <- rnorm(n = n_members, mean = mean(targets$variable %in% var, na.rm = T), sd = ic_sd)
   hist(ic_uc)
  
   param_df$sigma <- sigma
   param_df$ic_sd <- ic_sd
  
   #return(param_df)
  
  #-------------------------------------
  
  # Set up forecast data frame
  
  message('Make forecast dataframe')
  
  forecast_date_adjust <- forecast_date - lubridate::days(3) 
  
  #establish forecasted dates
  forecasted_dates <- seq(from = ymd(forecast_date_adjust), to = ymd(forecast_date_adjust) + forecast_horizon, by = "day")
  
  #get current spcond value
  #curr_spcond <- current_value(dataframe = targets,variable = var, start_date = forecast_date_adjust)
  curr_spcond_1 <- current_value(dataframe = targets,variable = var, start_date = forecast_date_adjust - lubridate::days(1))
  curr_spcond_2 <- current_value(dataframe = targets,variable = var, start_date = forecast_date_adjust - lubridate::days(2))
  curr_spcond_3 <- current_value(dataframe = targets,variable = var, start_date = forecast_date_adjust - lubridate::days(3))
  
  #set up df of different initial conditions for IC uncert
  # ic_df <- tibble(date = rep(as.Date(forecast_date), times = n_members),
  #                 ensemble_member = c(1:n_members),
  #                 forecast_variable = var,
  #                 value = rnorm(n = n_members, mean = curr_spcond, sd = 0.05),
  #                 uc_type = "initial")
  
  #set up df of different initial conditions for IC uncert
  ic_df_1 <- tibble(date = rep(as.Date(forecast_date)- lubridate::days(1), times = n_members),
                  ensemble_member = c(1:n_members),
                  forecast_variable = var,
                  value = rnorm(n = n_members, mean = curr_spcond_1, sd = 0.1),
                  uc_type = "initial")
  
  #set up df of different initial conditions for IC uncert
  ic_df_2 <- tibble(date = rep(as.Date(forecast_date) - lubridate::days(2), times = n_members),
                  ensemble_member = c(1:n_members),
                  forecast_variable = var,
                  value = rnorm(n = n_members, mean = curr_spcond_2, sd = 0.1),
                  uc_type = "initial")
  #set up df of different initial conditions for IC uncert
  ic_df_3 <- tibble(date = rep(as.Date(forecast_date) - lubridate::days(3), times = n_members),
                    ensemble_member = c(1:n_members),
                    forecast_variable = var,
                    value = rnorm(n = n_members, mean = curr_spcond_3, sd = 0.1),
                    uc_type = "initial")
  
  #set up table to hold forecast output 
  forecast_full_unc <- tibble(date = rep(forecasted_dates, times = n_members),
                              ensemble_member = rep(1:n_members, each = length(forecasted_dates)),
                              reference_datetime = forecast_date,
                              Horizon = date - reference_datetime,
                              forecast_variable = var,
                              value = as.double(NA),
                              uc_type = "initial") |>
    #rows_update(ic_df, by = c("date","ensemble_member","forecast_variable", "uc_type")) |>
    rows_update(ic_df_1, by = c("date","ensemble_member","forecast_variable", "uc_type")) |>
    rows_update(ic_df_2, by = c("date","ensemble_member","forecast_variable", "uc_type")) |>
    rows_update(ic_df_3, by = c("date","ensemble_member","forecast_variable", "uc_type"))
    # adding IC uncert
  
  
  #-------------------------------------
  
  ### ABP issue. Why can't I get forecast output values. Need to figure out how to get multiple days lagged for input. 
  # My initial conditional uncertainty is wayyy to big 
  
  message('Generating forecast')
  
  print(paste0('Running forecast starting on: ', forecast_date))
  
  #for loop to run forecast 
  for(i in 4:length(forecasted_dates)) {
    
    #pull prediction dataframe for the relevant date
    spcond_pred <- forecast_full_unc %>%
      filter(date == forecasted_dates[i])
    
    
    inflow_drive <- forecast_inflow %>%
      filter(as.Date(reference_datetime) == forecast_date) |> 
      filter(as.Date(datetime_date) == forecasted_dates[i]) |>
      slice(1:31)
    
    #pull lagged 1 spcond values
    spcond_lag <- forecast_full_unc %>%
      filter(date == forecasted_dates[i-1])
    
    #pull lagged 2 spcond values
   spcond_lag_2 <- forecast_full_unc %>%
     filter(date == forecasted_dates[i-2])
    
    #pull lagged 3 spcond values
   spcond_lag_3 <- forecast_full_unc %>%
     filter(date == forecasted_dates[i-3])
    
    #run model. # parameter uncertainty very high! Just using the ensemble from the water residence time for the uncertainity
    spcond_pred$value <- coeffs[1] + (inflow_drive$log_WRT_days_forecast * coeffs[2])  +
      (spcond_lag$value * coeffs[3]) + (spcond_lag_2$value * coeffs[4]) +
      (spcond_lag_3$value * coeffs[5]) +
      rnorm(n = 31, mean = 0, sd = sigma) #process uncert
    
    # spcond_pred$value <- param_df$beta_int + (inflow_drive$log_WRT_days_forecast * param_df$beta_WRT)  +
    #   (spcond_lag$value * param_df$beta_sp_lag1) + (spcond_lag_2$value * param_df$beta_sp_lag2) +
    #   (spcond_lag_3$value * param_df$beta_sp_lag3) +
    #   rnorm(n = 31, mean = 0, sd = sigma) #process uncert
    # 
    # 
    #insert values back into the forecast dataframe
    forecast_full_unc <- forecast_full_unc %>%
      rows_update(spcond_pred, by = c("date","ensemble_member","forecast_variable","uc_type"))
    
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
    filter(datetime >= forecast_date) |>
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






