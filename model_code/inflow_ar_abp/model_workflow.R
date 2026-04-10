# Run inflow model and submit to VERA forecasting challenge
#Author: ABP from DWH (organized be ADD)
#Date: 29July2024

#Purpose:create function to create nnetar model predictions for water quality variables

library(tidyverse)
library(lubridate)
library(vera4castHelpers)
library(zoo)

if(exists("curr_reference_datetime") == FALSE){
  
  curr_reference_datetime <- Sys.Date()
  
}else{
  
  print('Running Reforecast')
  
}

#Load data formatting functions
helper.functions <- list.files("./R/inflow_ar_abp")
sapply(paste0("./R/inflow_ar_abp/", helper.functions),source,.GlobalEnv)


#### set function inputs
## CHANGE FIRST TWO
forecast_date = Sys.Date() - lubridate::days(1)     # Date object: the reference date for this forecast
  forecast_horizon = 34     # integer: number of days ahead (e.g. 34)
  n_members = 31            # integer: number of ensemble members (e.g. 31)
  model_id = "inflow_ar_abp"            # string: model identifier for vera output
  inflow_targets_url = "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"  # vera4cast daily-inflow-targets.csv.gz URL
  met_targets_url = "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"      # vera4cast daily-met-targets.csv.gz URL
  noaa_4cast_url = "bio230121-bucket01/flare/drivers/met/gefs-v12/stage2"      # S3 bucket path: "bio230121-bucket01/flare/drivers/met/gefs-v12/stage2"
  calibration_start_date = as.Date("2015-07-07")
  project_id   = "vera4cast"
  var = "Flow_cms_mean"
  site = "tubr" 

 
  #output_folder <- paste0("./model_output/inflow_ar_abp/", model_id, "_", site, "_", forecast_date, ".csv")
  
  ##run function
  forecast_output <- generate_inflow_forecast(forecast_date = forecast_date, 
                                            forecast_horizon = forecast_horizon, 
                                            n_members = n_members,
                                            model_id = model_id, 
                                            inflow_targets_url = inflow_targets_url,   # vera4cast daily-inflow-targets.csv.gz URL
                                            met_targets_url = met_targets_url,      # vera4cast daily-met-targets.csv.gz URL
                                            noaa_4cast_url = noaa_4cast_url,
                                            var = var,
                                            site = site, 
                                            project_id = project_id, 
                                            calibration_start_date = calibration_start_date )
  
  
  # Write the file locally
  forecast_file_abs_path <- paste0("./model_output/inflow_ar_abp/", model_id, "_", site, "_", forecast_date, ".csv")
  
  # write to file
  print('Writing File...')
  
  if (!file.exists("./model_output/inflow_ar_abp/")){
    dir.create("./model_output/inflow_ar_abp/")
  }
  
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
  
 # end loop
