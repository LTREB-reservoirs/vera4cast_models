# Run fdom model and submit to VERA forecasting challenge
#Author: DWH (organized be ADD)
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
helper.functions <- list.files("./R/fdom_dwh")
sapply(paste0("./R/fdom_dwh/", helper.functions),source,.GlobalEnv)


#### set function inputs
## CHANGE FIRST TWO
#forecast_date <- Sys.Date() - lubridate::days(1)
forecast_date <- Sys.Date()
site <- "fcre"
forecast_depths <- 'focal'

forecast_horizon <- 16
n_members <- 31
calibration_start_date <- ymd("2022-11-11")
model_id <- "fdom_AR_dwh"
targets_url <- "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"

water_temp_4cast_old_url <- "bio230121-bucket01/vt_backup/forecasts/parquet/"

#water_temp_4cast_new_url <- 'focal' 

noaa_4cast_url <- "bio230121-bucket01/flare/drivers/met/gefs-v12/stage2"

var <- "fDOM_QSU_mean"
project_id <- "vera4cast"

output_folder <- paste0("./model_output/fdom_dwh/", model_id, "_", site, "_", forecast_date, ".csv")




##run function
forecast_output <- generate_fDOM_forecast(forecast_date = forecast_date, 
                       forecast_horizon = forecast_horizon, 
                       n_members = n_members,
                       output_folder = output_folder, 
                       model_id = model_id, 
                       targets_url = targets_url,
                       water_temp_4cast_old_url = water_temp_4cast_old_url,
                       # water_temp_4cast_new_url = water_temp_4cast_new_url,
                       noaa_4cast_url = noaa_4cast_url, 
                       var = var,
                       site = site, 
                       forecast_depths = forecast_depths, 
                       project_id = project_id, 
                       calibration_start_date = calibration_start_date )

doc_output <- forecast_output |> 
  mutate(variable = 'DOC_mgL_sample',
         prediction = ((0.2697*prediction) + 0.4675))

fdom_doc_output <- dplyr::bind_rows(forecast_output, doc_output)

# Write the file locally
forecast_file_abs_path <- paste0("./model_output/fdom_dwh/", model_id, "_", site, "_", forecast_date, ".csv")

# write to file
print('Writing File...')

if (!file.exists("./model_output/fdom_dwh/")){
  dir.create("./model_output/fdom_dwh/")
}

write.csv(fdom_doc_output, forecast_file_abs_path, row.names = FALSE)


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