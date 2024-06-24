#Format data for each model and fit models from 2018-2021
#Author: Austin Delany
#Date: 15Apr2024

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
data.format.functions <- list.files("./R/fable_NNETAR_focal")
sapply(paste0("./R/fable_NNETAR_focal/", data.format.functions),source,.GlobalEnv)

#Define targets filepath
targets <- "https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"

#target_variable <- 'Chla_ugL_mean'
#target_variable <- 'Temp_C_mean'
#target_variable <- 'DO_mgL_mean'
#target_variable <- 'Secchi_m_sample'

target_variables <- c('Temp_C_mean', "DO_mgL_mean", "Secchi_m_sample", "fDOM_QSU_mean")

prediction_df <- data.frame()

for (t in target_variables){
  
  print(t)
  
  #Define start and end dates (needed for interpolation)
  end_date = curr_reference_datetime
  
  #Format data
  dat_NNETAR <- format_data_NNETAR(targets = targets,
                                   target_var = t,
                                   end_date = end_date)
  
  #Set prediction window and forecast horizon
  reference_datetime <- curr_reference_datetime
  forecast_horizon = 35
  
  #Predict variable
  pred <- fableNNETAR(data = dat_NNETAR,
                      target_var = t,
                      reference_datetime = reference_datetime,
                      forecast_horizon = forecast_horizon)
  
  # calculate probability of bloom -- if target variables include chla
  if (t %in% c('Chla_ugL_mean')){
    mod <- pred %>%
      mutate(bloom = ifelse(prediction >= 20, 1, 0)) %>%
      group_by(site_id, datetime, reference_datetime, family, variable, model_id, duration, project_id, depth_m) %>%
      summarize(prediction = sum(bloom)/1000) %>%
      mutate(family = "bernoulli",
             variable = "Bloom_binary_mean") %>%
      add_column(parameter = "prob")
    
    fc <- bind_rows(pred, mod)
    
    pred <- fc
    
    print('Bloom_binary_mean')
  }
  
  prediction_df <- bind_rows(prediction_df, pred)
  
} # close variable iteration loop

# Submit forecasts
theme <- 'daily'
date <- curr_reference_datetime

forecast_models <- c("fableNNETAR_focal")

forecast_name <- c(paste0(forecast_models, ".csv"))

# Write the file locally
forecast_file <- paste(theme, date, forecast_name, sep = '-')

forecast_file_abs_path <- paste0("./model_output/fable_NNETAR_focal/",forecast_file)

# write to file
print('Writing File...')

if (!file.exists("./model_output/fable_NNETAR_focal")){
  dir.create("./model_output/fable_NNETAR_focal")
}
write.csv(prediction_df, forecast_file_abs_path, row.names = FALSE)

# validate
print('Validating File...')
vera4castHelpers::forecast_output_validator(forecast_file_abs_path)
vera4castHelpers::submit(forecast_file_abs_path, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)

#Sys.sleep(60)

# for(i in 1:length(forecast_names)){
#   
#   # Write the file locally
#   forecast_file <- paste(theme, date, forecast_names[i], sep = '-')
# 
#   forecast_file1 <- paste0("./model_output/",forecast_file)
#   
#   # write to file
#   write.csv(pred, forecast_file1, row.names = FALSE)
#   
#   # validate
#   vera4castHelpers::forecast_output_validator(forecast_file1)
#   vera4castHelpers::submit(forecast_file1, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)
#   Sys.sleep(60)
# }


