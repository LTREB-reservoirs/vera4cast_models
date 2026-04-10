# Run random forest model for spcond and submit to VERA forecasting challenge
#Author: ABP
#Date: 02 April 2026

#Purpose:create function to create random forest model predictions for water quality variables

library(tidyverse)
library(lubridate)
library(vera4castHelpers)
library(zoo)
library(randomForest)

if(exists("curr_reference_datetime") == FALSE){
  
  curr_reference_datetime <- Sys.Date()
  
}else{
  
  print('Running Reforecast')
  
}

#Load data formatting functions
helper.functions <- list.files("./R/spcond_rf_abp")
sapply(paste0("./R/spcond_rf_abp/", helper.functions),source,.GlobalEnv)


#### set function inputs
## CHANGE FIRST TWO
forecast_date <- Sys.Date() - lubridate::days(1)
site <- "fcre"
forecast_depths <- 'focal'

forecast_horizon <- 34
calibration_start_date <- ymd("2019-01-01")
model_id <- "spcond_RF_abp"
targets_url <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"
target_url_inflow <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"

wtr_4cast_url <- "bio230121-bucket01/vt_backup/forecasts/parquet/"

inflow_4cast_url <- 'bio230121-bucket01/vera4cast/forecasts/archive-parquet/project_id=vera4cast/duration=P1D'

var <- "SpCond_uScm_mean"
project_id <- "vera4cast"
  
  output_folder <- paste0("./model_output/spcond_rf_abp/", model_id, "_", site, "_", forecast_date, ".csv")
  
  ##run function
  forecast_output <- generate_spcond_rf_forecast(forecast_date = forecast_date, 
                                            forecast_horizon = forecast_horizon, 
                                            output_folder = output_folder, 
                                            model_id = model_id, 
                                            targets_url = targets_url,
                                            target_url_inflow = target_url_inflow,
                                            inflow_4cast_url = inflow_4cast_url,
                                            wtr_4cast_url = wtr_4cast_url,
                                            var = var,
                                            site = site, 
                                            forecast_depths = forecast_depths, 
                                            project_id = project_id, 
                                            calibration_start_date = calibration_start_date )
  
  
  # Write the file locally
  forecast_file_abs_path <- paste0("./model_output/spcond_rf_abp/", model_id, "_", site, "_", forecast_date, ".csv")
  
  # write to file
  print('Writing File...')
  
  if (!file.exists("./model_output/spcond_rf_abp/")){
    dir.create("./model_output/spcond_rf_abp/")
  
  
  write.csv(forecast_output, forecast_file_abs_path, row.names = FALSE)
  
  
  ## validate and submit forecast
  
  # validate
  print('Validating File...')
  vera4castHelpers::forecast_output_validator(forecast_file_abs_path)
  vera4castHelpers::submit(forecast_file_abs_path, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)
  
  
  # read.csv("C:/Users/dwh18/Downloads/fDOM_AR_dwh_fcre_2024-07-01.csv")|>
  #   mutate(date = as.Date(datetime)) |>
  #   # filter(forecast_date > ymd("2023-01-03")) |>
  #   ggplot(aes(x = date, y = prediction, color = as.character(parameter)))+
  #   geom_line()
  
} # end loop
