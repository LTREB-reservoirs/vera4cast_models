## example model for secchi -- taken from Katie's model code ##
library(vera4castHelpers)
library(tidyverse)

setwd(here::here())

# load the forecast generation function (source any related funcitons/scripts you might need here)
source('model_code/Dom_CO2/Dom_CO2_model_code.R')
#source('templates/get_weather.R') # wrapper around the RopenMeteo package to get weather covariates
source('R/scoring/generate_forecast_score.R')


# ---- Generate the forecasts -----
# default is to run a real-time forecast for today
forecast_date <- as.Date('2025-08-01') ## could call Sys.Date() here to run true forecast
model_id <- 'Dom_CO2' # your unique model name
#forecast_horizon <- 30 # how long should the forecast be?
target_variable <- 'CO2_umolL_sample' # variable you want to forecast
#forecast_site <- 'fcre'


targets_url = "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"


## Dom's code ##

reforecast_df <- Dom_CO2model_function(forecast_rundate = forecast_date,
                                       forecast_model_id = model_id,
                                       forecast_target = target_variable)

## old example from Katie
# reforecast_df <- generate_secchi_forecast(forecast_date, # a recommended argument so you can pass the date to the function
#                                           model_id,
#                                           targets_url, # where are the targets you are forecasting?
#                                           horizon = 30, #how many days into the future
#                                           forecast_variable = target_variable,
#                                           site = forecast_site, # what site
#                                           project_id = 'vera4cast') 




targets_compare_df <- read_csv(targets_url) |> 
  filter(variable == target_variable)

reforecast_score_df <- generate_forecast_score(targets_df = targets_compare_df,
                                               forecast_df = reforecast_df)

source('templates/plot_function.R')
generate_forecast_plot(score_df = reforecast_score_df, 
                       y_limits = c(0,40))
