# Run DCM depth model and submit to VERA forecasting challenge
# Author: MP
# Date: February 2025

# Purpose: forecast Deep Chlorophyll Maximum (DCM) depth using
# thermocline depth, Schmidt stability, and Secchi depth (photic zone proxy) as covariates

library(tidyverse)
library(lubridate)
library(vera4castHelpers)
library(zoo)

if(exists("curr_reference_datetime") == FALSE){

  curr_reference_datetime <- Sys.Date()

}else{

  print('Running Reforecast')

}

# Load helper functions
helper.functions <- source("./R/fDCMdepth_mp/helper_functions.R")

#### set function inputs
forecast_date <- Sys.Date() - lubridate::days(1)
sites <- c("fcre") #maybe can change to just one or the other
forecast_depths <- 'focal'

forecast_horizon <- 34
n_members <- 31
calibration_start_date <- ymd("2022-11-11")
model_id <- "fDCMdepth_mp"
targets_url <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"

var <- "ChlorophyllMaximum_depth_sample"
project_id <- "vera4cast"


for (i in sites){

  site <- i
  print(site)

  output_folder <- paste0("./model_output/fDCMdepth_mp/", model_id, "_", site, "_", forecast_date, ".csv")

  ## run function
  forecast_output <- generate_DCMdepth_forecast(forecast_date = forecast_date,
                                                forecast_horizon = forecast_horizon,
                                                n_members = n_members,
                                                output_folder = output_folder,
                                                model_id = model_id,
                                                targets_url = targets_url,
                                                var = var,
                                                site = site,
                                                forecast_depths = forecast_depths,
                                                project_id = project_id,
                                                calibration_start_date = calibration_start_date)

  # Write the file locally
  forecast_file_abs_path <- paste0("./model_output/fDCMdepth_mp/", model_id, "_", site, "_", forecast_date, ".csv")

  print('Writing File...')

  if (!file.exists("./model_output/fDCMdepth_mp/")){
    dir.create("./model_output/fDCMdepth_mp/")
  }

  write.csv(forecast_output, forecast_file_abs_path, row.names = FALSE)


  ## validate and submit forecast (uncomment when ready to submit)
  # print('Validating File...')
  # vera4castHelpers::forecast_output_validator(forecast_file_abs_path)
  # vera4castHelpers::submit(forecast_file_abs_path, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)

} # end loop

#forecast_output_validator()
#submit()
#Vera4castHelpers functions
#need to register model


library(ggplot2)
library(dplyr)
library(patchwork)

# Compute mean + 90% ribbon
summary_df <- forecast_output |>
  group_by(datetime) |>
  summarise(
    mean_pred  = mean(prediction, na.rm = TRUE),
    lower      = quantile(prediction, 0.05, na.rm = TRUE),
    upper      = quantile(prediction, 0.95, na.rm = TRUE),
    .groups = "drop"
  )

# 1. Spaghetti plot
p1 <- forecast_output |>
  mutate(parameter = as.factor(parameter)) |>
  ggplot(aes(x = datetime, y = prediction, group = parameter)) +
  geom_line(alpha = 0.25, linewidth = 0.4, color = "#1D9E75") +
  labs(
    title    = "DCM Depth Forecast - Ensemble members",
    subtitle = paste0("Reference: ", unique(forecast_output$reference_datetime), " · fcre · n=", n_distinct(forecast_output$parameter)),
    x        = NULL,
    y        = "DCM Depth (m)"
  ) +
  scale_y_reverse() +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank())

# 2. Mean + ribbon
p2 <- summary_df |>
  ggplot(aes(x = datetime)) +
  geom_ribbon(aes(ymin = lower, ymax = upper), fill = "#1D9E75", alpha = 0.2) +
  geom_line(aes(y = mean_pred), color = "#1D9E75", linewidth = 1) +
  labs(
    title    = "DCM Depth Forecast - Ensemble mean ± 90% interval",
    subtitle = paste0("Reference: ", unique(forecast_output$reference_datetime), " · fcre"),
    x        = "Date",
    y        = "DCM Depth (m)"
  ) +
  scale_y_reverse() +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank())

# Stack them
p1 / p2

