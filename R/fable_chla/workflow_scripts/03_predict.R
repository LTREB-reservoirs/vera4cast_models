#Predict chl-a
#Author: Mary Lofton
#Date: 26SEP23

#Purpose: predict EXO chl-a 30 days into future at FCR and BVR

library(tidyverse)
library(lubridate)

# Install VERA helpers package if have not already done so
library(vera4castHelpers)

#Load prediction functions
data.format.functions <- list.files("./model_code/R/fable_chla/function_library/predict")
sapply(paste0("./model_code/R/fable_chla/function_library/predict", data.format.functions),source,.GlobalEnv)

# predict.model.functions <- list.files("./code/function_library/predict")
# sapply(paste0("./code/function_library/predict/",predict.model.functions),source,.GlobalEnv)

#Read in data
dat_ETS <- read_csv("./model_output/fable_chla/processed_targets/ETS.csv")
dat_ARIMA <- read_csv("./model_output/fable_chla/processed_targets/ARIMA.csv")
dat_NNETAR <- read_csv("./model_output/fable_chla/processed_targets/NNETAR.csv")


#Set prediction window and forecast horizon
reference_datetime <- curr_reference_datetime
forecast_horizon = 35
pred <- list()

#Predict chl-a
pred[[1]] <- fableETS(data = dat_ETS,
                     reference_datetime = reference_datetime,
                     forecast_horizon = forecast_horizon)
pred[[2]] <- fableARIMA(data = dat_ARIMA,
                     reference_datetime = reference_datetime,
                     forecast_horizon = forecast_horizon)
pred[[3]] <- fableNNETAR(data = dat_NNETAR,
                     reference_datetime = reference_datetime,
                     forecast_horizon = forecast_horizon)

# Submit chl-a forecasts
theme <- 'daily'
date <- curr_reference_datetime

forecast_models <- c("fableETS","fableARIMA","fableNNETAR")

forecast_names <- c(paste0(forecast_models, ".csv"))

for(i in 1:length(forecast_names)){

# Write the file locally
forecast_file <- paste(theme, date, forecast_names[i], sep = '-')
forecast_file

forecast_file1 <- paste0("./model_output/fable_chla/",forecast_file)
forecast_file1

# calculate probability of bloom
mod <- pred[[i]] %>%
  mutate(bloom = ifelse(prediction >= 20, 1, 0)) %>%
  group_by(site_id, datetime, reference_datetime, family, variable, model_id, duration, project_id, depth_m) %>%
  summarize(prediction = sum(bloom)/1000) %>%
  mutate(family = "bernoulli",
         variable = "Bloom_binary_mean") %>%
  add_column(parameter = "prob")

fc <- bind_rows(pred[[i]],mod)

# write to file
write.csv(fc, forecast_file1, row.names = FALSE)

# validate
vera4castHelpers::forecast_output_validator(forecast_file1)
vera4castHelpers::submit(forecast_file1, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)
Sys.sleep(60)
}

