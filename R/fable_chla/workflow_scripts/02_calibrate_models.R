#Format data for each model and fit models for historical period
#Author: Mary Lofton
#Date: 26SEP23

#Purpose: calibrate models to FCR + BVR historical data

library(tidyverse)
library(lubridate)

#Load model fitting functions
fit.model.functions <- list.files("./code/function_library/fit_models")
sapply(paste0("./code/function_library/fit_models/",fit.model.functions),source,.GlobalEnv)

#Read in data
dat_ETS <- read_csv("./data/processed_targets/ETS.csv")
dat_ARIMA <- read_csv("./data/processed_targets/ARIMA.csv")
dat_NNETAR <- read_csv("./data/processed_targets/NNETAR.csv")


#Fit models (not applicable for persistence model)
fit_ETS <- fit_ETS(data = dat_ETS)
fit_ETS$plot

fit_ARIMA <- fit_ARIMA(data = dat_ARIMA)
fit_ARIMA$plot

fit_NNETAR <- fit_NNETAR(data = dat_NNETAR)
fit_NNETAR$plot

#Stack model predictions and write to file 
mod_output <- bind_rows(fit_ETS$out,
                        fit_ARIMA$out,
                        fit_NNETAR$out)

#OR if you only want to run (or re-run) one or a few models
mod_output <- read_csv("./multi-model-ensemble/model_output/calibration_output.csv") %>%
  #filter(!model_id %in% c("OptimumMonod")) %>% #names of re-run models if applicable
  bind_rows(.,fit_prophet$out) # %>% #bind rows with models to add/replace if applicable

write.csv(mod_output, "./model_output/calibration_output.csv", row.names = FALSE)

