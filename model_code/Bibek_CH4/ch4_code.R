#pacman::p_load(readr, tidyverse, openair, ggplot2, dplyr, lattice, arrow, magick, lubridate)

## model set up 

forecast_date <- as.Date('2025-08-25')
horizon = 30



#read in the targets data
url <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"

targets <- read_csv(url, show_col_types = FALSE)

url_met <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"

targets_met <- read_csv(url_met, show_col_types = FALSE)

site_list <- read_csv("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast/main/vera4cast_field_site_metadata.csv",
                      show_col_types = FALSE)

site_list <- site_list %>%
  filter(site_id == 'fcre')

targets <- targets %>%
  filter(site_id == 'fcre')



## DEFINE WEATHER DRIVERS 

lat <- site_list |>
  filter(site_id == 'fcre') |>
  select(latitude) |> 
  pull()

long <-  site_list |>
  filter(site_id == 'fcre') |>
  select(longitude) |>  
  pull()


max_horizon <- forecast_date + lubridate::days(horizon)

weather_dat <- vera4castHelpers::noaa_stage3() |> 
  filter(datetime <= max_horizon,
         datetime >=  forecast_date, 
         site_id == 'fcre') |> 
  collect() |> 
  mutate(reference_datetime = forecast_date) |> 
  filter(variable == c("air_temperature",
                       "eastward_wind",
                       "northward_wind")) |>
  mutate(windspeed = sqrt(eastward_wind^2 + northward_wind^2))

# weather_dat <- ropenmeteo::get_ensemble_forecast(
#   latitude = lat,
#   longitude = long,
#   forecast_days = 30, # days into the future
#   past_days = 60, # past days that can be used for model fitting
#   model = "gfs_seamless", # this is the NOAA gefs ensemble model
#   variables = c("temperature_2m",
#                 "wind_speed_10m")) |>
#   
#   # function to convert to EFI standard
#   ropenmeteo::convert_to_efi_standard() |>
#   mutate(site_id = 'fcre')


## RUN YOUR MODEL FUNCTION 
source('./R/Bibek_CH4/example_CH4_model.R')

example_CH4_model()