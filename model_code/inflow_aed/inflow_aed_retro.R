# Workflow script
# Author: Austin Delany
# Date: 17Apr2024

# Purpose: run forecasting workflow for VERA

library(tidyverse)


# check for any missing forecasts
message("==== Checking for missed forecasts ====")
challenge_model_name <- 'inflow_gefsClimAED'

# Dates of forecasts 
today <- paste(Sys.Date() - days(2), '00:00:00')
# this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2025-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
#                         exists = NA)

this_year <- data.frame(date = as.Date(seq.Date(as_date('2025-07-01'), to = as_date(today), by = 'day')))

# check inflow forecast dates
s3 <- arrow::s3_bucket(bucket = glue::glue("bio230121-bucket01/vera4cast/forecasts/parquet/project_id=vera4cast/duration=P1D/variable=Temp_C_mean/model_id=inflow_gefsClimAED"),
                       endpoint_override = "https://amnh1.osn.mghpcc.org",
                       anonymous = TRUE)

avail_dates <- gsub("reference_date=", "", s3$ls())

this_year$exists <- ifelse(this_year$date %in% avail_dates, T, F)

rerun_dates <- this_year |> filter(exists == FALSE) |> pull(date)


if (length(rerun_dates) != 0){ ## CHECK IF THERE ARE MISSING DATES FOUND
  
  for (i in rerun_dates){
    
    source('./model_code/inflow_aed/inflow_aed_retro_function.R')
    
    message(paste0('Remaking forecast for ', as.Date(i)))
    
    ## RERUN FORECAST HERE ##
    inflow_aed_retro_function(i)
    
  }
  
  
} else{
  message('NO MISSING FORECASTS FOUND')
}
