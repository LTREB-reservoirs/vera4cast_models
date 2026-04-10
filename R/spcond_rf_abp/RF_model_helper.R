#### Function to run sp.cond Random forest model forecasts for VERA
#### Heavily borrowed from DWH fdom AR model
## ABP: Feb. 2026
### Edited: 2 April 2026

library(tidyverse)
library(arrow)
library(randomForest)
#library(caret)




generate_spcond_rf_forecast <- function(forecast_date, # a recommended argument so you can pass the date to the function
                                     forecast_horizon,
                                     output_folder,
                                     calibration_start_date,
                                     model_id,
                                     targets_url, # where are the targets you are forecasting?
                                     target_url_inflow,
                                     wtr_4cast_url,
                                     inflow_4cast_url,
                                     var, # what variable(s)?
                                     site, # what site(s),
                                     forecast_depths = 'focal',
                                     project_id = 'vera4cast') {
  
  # forecast_date <- Sys.Date() - lubridate::days(1)
  # #forecast_date <- Sys.Date()
  # site <- "fcre"
  # forecast_depths <- 'focal'
  # 
  # forecast_horizon <- 34
  # n_members <- 31
  # calibration_start_date <- ymd("2019-01-01")
  # model_id <- "spcond_RF_abp"
  # targets_url <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"
  # target_url_inflow <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"
  # 
  # wtr_4cast_url <- "bio230121-bucket01/vt_backup/forecasts/parquet/"
  # #water_temp_4cast_old_url <- "bio230121-bucket01/vt_backup/forecasts/parquet/"
  # 
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
 # message('Getting targets')
  # # use the targets files
  targets_insitu <- readr::read_csv(targets_url, show_col_types = F)|>
    filter(variable %in% c("SpCond_uScm_mean", "Temp_C_mean"),
           site_id %in% "fcre",
           depth_m %in% 1.6,
           datetime <= forecast_date)|>
     select(datetime, variable, observation) #|>
    # pivot_wider(names_from = variable, values_from = observation)|>
    #   drop_na()|>
    #   arrange(datetime)

  # getting inflow targets
  targets_inflow <- readr::read_csv(target_url_inflow, show_col_types = F)|>
    filter(variable %in% "Flow_cms_mean",
           datetime <= forecast_date)|>
    select(datetime, variable, observation)

  # make a data frame of the targets

  train_df <- bind_rows(targets_insitu, targets_inflow)|>
    pivot_wider(names_from = variable, values_from = observation)|>
    drop_na()|>
    arrange(datetime)
  
  
  #-------------------------------------
  
  #Get inflow forecasts
  message('Getting inflow 4casts')
  
  # Here is the code for accessing FCR inflow forecasts. The way it is set up will take some time to complete, but you can add additional filters (variable, reference_datetime, etc.) which will speed things up
  
  # Get the water temp forecasts
  #water_temp_4cast_new_url <- "s3://anonymous@bio230121-bucket01/vera4cast/forecasts/parquet/project_id=vera4cast/duration=P1D/variable=Temp_C_mean?endpoint_override=renc.osn.xsede.org"
  
  # fcre_wt_forecasts <- arrow::s3_bucket(file.path("bio230121-bucket01/vt_backup/forecasts/parquet/site_id=fcre/model_id=test_runS3/"),
  #                                       endpoint_override = 'amnh1.osn.mghpcc.org',
  #                                       anonymous = TRUE)
  # 
  # df_flare_old <- arrow::open_dataset(fcre_wt_forecasts) |>
  #   #dplyr::filter(#depth == 1.6,
  #                 #variable == 'temperature') |>
  #   dplyr::collect()
                                        
  
  
  
  fcre_reforecast <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/forecasts/parquet/site_id=fcre/model_id=glm_aed_flare_v3/"),
                                      endpoint_override = 'amnh1.osn.mghpcc.org',
                                      anonymous = TRUE)
  
  #new_flare_forecasts <- arrow::open_dataset(fcre_reforecast)
  
  df_flare_new <- arrow::open_dataset(fcre_reforecast) |>
    dplyr::filter(depth == 1.6, 
                  variable == 'Temp_C_mean') |> 
    dplyr::collect()
  
  
  
  
  
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
    collect()|>
    mutate(parameter = as.numeric(parameter))
  
  
  df_inflow_forecast <- bind_rows(df_flare_old, df_flare_new)|> 
    rename(datetime_date = datetime) |> 
    # filter(parameter <= 31,
    #        site_id == site) |> 
    mutate(site_id = site)|>
    # model_id = 'test_runs3') |> 
    filter(as.Date(reference_datetime) > ymd("2022-11-07") ) |>  #remove odd date that has dates one month behind reference datetime
    select(reference_datetime, datetime_date, site_id, family, parameter, variable, prediction, model_id)
    # convert the Flow to residence time and then take the log of that 
    # mutate(WRT_days_forecast = ( (3.1E5/prediction) * (1/60) * (1/60) * (1/24) ),
    #        log_WRT_days_forecast = log(WRT_days_forecast))
  
  
  
  
  # split it into historic and future
  historic_inflow <- df_inflow_forecast |>
    filter(as.Date(datetime_date) == as.Date(reference_datetime)) |> 
    # calculate a daily mean (remove ensemble)
    group_by(reference_datetime, variable) |>
    summarise(prediction = mean(prediction, na.rm = T), .groups = "drop") |>
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
  
  fit_df <- train_df |>
    filter(datetime < forecast_date,
           datetime >= calibration_start_date)|> ## THIS is the furthest date that we have all values for 
    #left_join(targets_insitu) |>
    drop_na()
  
  # Split data into training and test sets
  #testData <- fit_df
  
  # # split between test and training data
  trainIndex <- sample(1:nrow(fit_df), 0.8 * nrow(fit_df))

  trainData <- fit_df[trainIndex, ]
  testData <- fit_df[-trainIndex, ]
  
  # run the rf model
  spcond_rf_model <- randomForest(SpCond_uScm_mean ~ Flow_cms_mean + Temp_C_mean, data = trainData, ntree = 500, mtry = 2)
  
 print(spcond_rf_model)
 
 # use the rfmodel and the forecasted temp and inflow to predict spcond
 
 testData$forecast_spcond <- predict(spcond_rf_model, testData)
 
 
 # ggplot(testData, aes(x = datetime)) +
 #   geom_point(aes(y = SpCond_uScm_mean))+
 #   geom_point(aes(y = forecast_spcond), color = "red")+
 #   theme_bw()
  
  
  ####get process uncertainty 
 # will use process uncertainity for this
  #find residuals. Not using the process uncertainty right now
  #fit_df_noNA <- na.omit(testData) # take out any NAs from the targets file
  #mod <- predict(spcond_model, data = fit_df_noNA)
  residuals <- testData$forecast_spcond - testData$SpCond_uScm_mean
  sigma <- sd(residuals, na.rm = TRUE) # Process Uncertainty Noise Std Dev.; this is your sigma
  
  # now get the forecast data in order
  
  forecast_input <- forecast_inflow|>
    select(reference_datetime, datetime_date, parameter, variable, prediction)|>
    group_by(datetime_date, variable)|>
    summarise(pred = mean(prediction, na.rm =T))|>
    ungroup()|>
    pivot_wider(names_from = variable, values_from = pred)|>
    drop_na()
  
  message('Make forecast')
  # now use the rf model to make predictions with the forecasted water temp and inflow
  
  forecast_input$mu <- round(predict(spcond_rf_model, forecast_input), digits = 2)
  
  # add in the sigma column 
  forecast_input$sigma <- round(sigma, digits = 2)
  
  forecast_par <- forecast_input|>
    select(datetime_date, mu, sigma)|>
    pivot_longer(!datetime_date, names_to = "parameter", values_to = "prediction")
  
  
  #-------------------------------------
  
 
  
 message("Clean up the file and save")
  
  #clean up file to match vera format 
  
  forecast_df <- forecast_par |>
    rename(datetime = datetime_date) |>
    mutate(family = 'normal',
           duration = "P1D",
           depth_m = forecast_depths,
           variable = var,
           project_id = project_id,
           model_id = model_id,
           site_id = site,
           reference_datetime = forecast_date
    ) |>
    select(datetime, reference_datetime, duration, site_id, depth_m, 
           family, parameter, variable, prediction, model_id, project_id)
  
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






