# Workflow script
# Author: Austin Delany
# Date: 17Apr2024

# Purpose: run forecasting workflow for VERA

# install R packages that aren't already in neon4cast rocker
install.packages("remotes")
install.packages("tidyverse")
install.packages("lubridate")
install.packages("zoo")
install.packages("fable")
install.packages("feasts")
install.packages("urca")
library(remotes)
remotes::install_github("LTREB-reservoirs/vera4castHelpers", force = TRUE)

library(tidyverse)
library(tsibble)
library(aws.s3)

# check for any missing forecasts
message("==== Checking for missed forecasts ====")
challenge_model_name <- 'fableNNETAR_focal'

# Dates of forecasts 
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2024-04-26'), to = as_date(today), by = 'day'), ' 00:00:00')),
                        exists = NA)

# vera bucket
s3 <- arrow::s3_bucket("bio230121-bucket01/vera4cast/inventory/catalog",
                       endpoint_override = "amnh1.osn.mghpcc.org",
                       anonymous = TRUE)

#submitted_forecasts_df <- arrow::open_dataset(s3) |> collect()

# is that forecast present in the bucket?
for (i in 1:nrow(this_year)) {
  
  s3_forecasts_df <- arrow::open_dataset(s3) |> 
    filter(reference_date == lubridate::as_datetime(this_year$date[i])) |> 
    select(model_id) |> 
    collect()
  
  # models_submitted <- unique(submitted_forecasts_df %>%
  #                              filter(reference_date == this_year$date[i]) %>%
  #                              pull(model_id))
  
  models_submitted <- unique(s3_forecasts_df$model_id)
  
  this_year$exists[i] <- ifelse(challenge_model_name %in% models_submitted,T,F)
  
}

# which dates do you need to generate forecasts for?
# those that are missing or haven't been submitted
missed_dates <- this_year |> 
  filter(!(exists == T)) |> 
  pull(date) |> 
  as_date()

if (length(missed_dates) != 0) {
  for (i in 1:length(missed_dates)) {
    
    curr_reference_datetime <- missed_dates[i]
    
    message(paste("creating forecasts for",print(curr_reference_datetime)))
    
    # Script to run forecasts
    source("./model_code/fable_NNETAR_focal/nnetar_workflow.R")
    message('forecasts submitted!')
    
  }
} else {
  message('no missed forecasts')  
}



