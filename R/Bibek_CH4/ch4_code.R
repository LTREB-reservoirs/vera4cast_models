#Load libraries
pacman::p_load(readr, tidyverse, openair, ggplot2, dplyr, lattice, arrow, magick, lubridate, vera4castHelpers)


# SETUP 
forecast_date <- as.Date('2025-07-25')
horizon       <- 30
max_horizon   <- forecast_date + days(horizon)

# LOAD SITE METADATA
site_list <- read_csv("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast/main/vera4cast_field_site_metadata.csv",
                      show_col_types = FALSE) |>
                      filter(site_id == 'fcre')

lat  <- site_list$latitude
long <- site_list$longitude

# LOAD CH4 TARGETS
targets <- read_csv(
  "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz",
  show_col_types = FALSE
) |> 
  filter(site_id  == 'fcre',
         variable == 'CH4flux_umolm2s_mean',
         duration == 'P1D')

# LOAD HISTORICAL MET TARGETS FOR MODEL FITTING
targets_met <- read_csv(
  "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz", show_col_types = FALSE) |>
  filter(site_id == 'fcre')

# Combine CH4 and met targets for model fitting
targets_combined <- bind_rows(targets, targets_met)


# FUTURE WEATHER
future_weather <- noaa_stage3() |>
  filter(datetime >= forecast_date,
         datetime <= max_horizon,
         site_id  == 'fcre',
         variable %in% c("air_temperature",
                         "eastward_wind",
                         "northward_wind")) |>
  collect() |>
  
  # aggregate 1-minute data
  mutate(date = as.Date(datetime)) |>
  group_by(date, datetime, site_id, parameter, variable) |>
  summarise(prediction = mean(as.numeric(prediction), na.rm = TRUE),
            .groups = "drop") |>
  
  # THEN pivot
  pivot_wider(names_from = variable,
              values_from = prediction) |>
  
  # Now calculations work
  mutate(
    air_temperature = air_temperature - 273.15,
    wind_speed = sqrt(eastward_wind^2 + northward_wind^2)
  ) |>
  
  # Daily averages
  group_by(date, site_id, parameter) |>
  summarise(
    AirTemp_C_mean = mean(air_temperature, na.rm = TRUE),
    WindSpeed_ms_mean = mean(wind_speed, na.rm = TRUE),
    .groups = "drop"
  ) |>
  
  rename(datetime = date)


# SOURCE MODEL FUNCTION
#Github source
source("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast_models/main/R/Bibek_CH4/example_CH4_model_unc.R")
# #Local source
# source("C:/Users/13188/Desktop/vera4cast_models/R/Bibek_CH4/example_CH4_model_unc.R")

# RUN MODEL
reforecast_df <- example_CH4_model(
                                  forecast_date     = forecast_date,
                                  model_id          = 'beebake_CH4flux_ID',
                                  horizon           = horizon,
                                  forecast_variable = 'CH4flux_umolm2s_mean',
                                  site              = 'fcre',
                                  project_id        = 'vera4cast'
)

# GENERATE SCORES
targets_compare_df <- targets |>
                           filter(site_id  == 'fcre',
                           variable == 'CH4flux_umolm2s_mean')

#Github source
source("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast_models/main/R/scoring/generate_forecast_score.R")

reforecast_score_df <- generate_forecast_score(
                           targets_df  = targets_compare_df,
                           forecast_df = reforecast_df
)

# PLOT MODEL PERFORMANCE
#Github source
source("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast_models/main/templates/plot_function.R")

generate_forecast_plot(
                     score_df = reforecast_score_df,
                     y_limits = c(-0.05,0.1)
)













