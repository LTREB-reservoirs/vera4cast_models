## reforecast script for DCM depth model ##
library(devtools)
devtools::install_github('eco4cast/score4cast')
library(vera4castHelpers)
library(tidyverse)

setwd(here::here())

# load the forecast generation function (source any related functions/scripts you might need here)
source('R/fDCMdepth_mp/helper_functions.R')
source('R/fDCMdepth_mp/data_availability_function.R')
source('R/scoring/generate_forecast_score.R')
source('templates/plot_function.R')

#2024-09-25 is the latest date for FLARE output
#deepest observation since then is 2025-02-10 at 6.25 so I want to test 5 days prior

# ---- Generate the forecasts -----
forecast_date <- as.Date('2026-03-02') ## could call Sys.Date() here to run true forecast
model_id <- 'fDCMdepth_mp' # your unique model name
forecast_horizon <- 34 # how long should the forecast be?
n_members <- 31
target_variable <- 'ChlorophyllMaximum_depth_sample' # variable you want to forecast
forecast_site <- 'fcre'
calibration_start_date <- ymd("2017-11-11")

targets_url <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"

output_folder <- paste0("./model_output/fDCMdepth_mp/", model_id, "_", forecast_site, "_", forecast_date, ".csv")


reforecast_df <- generate_DCMdepth_forecast(forecast_date = forecast_date,
                                            forecast_horizon = forecast_horizon,
                                            n_members = n_members,
                                            output_folder = output_folder,
                                            calibration_start_date = calibration_start_date,
                                            model_id = model_id,
                                            targets_url = targets_url,
                                            var = target_variable,
                                            site = 'fcre',
                                            forecast_depths = 'focal',
                                            project_id = 'vera4cast')

targets_compare_df <- read_csv(targets_url) |>
  filter(variable == target_variable, site_id %in% c("fcre"))|>
  filter(datetime >= date('2024-09-25')) #this is the earliest date for flare availability


#depth_m is NA in both targets and forecast for this variable, but joins don't
#match on NA - assign a placeholder so the score join actually attaches obs
targets_compare_df <- targets_compare_df |> mutate(depth_m = 1.6)
reforecast_df      <- reforecast_df      |> mutate(depth_m = 1.6)

reforecast_score_df <- generate_forecast_score(targets_df = targets_compare_df,
                                               forecast_df = reforecast_df)


#plot the score df for ribbon/mean, but pull obs straight from targets so they
#are not dependent on the score4cast join lining up
plot_df <- reforecast_score_df |>
  mutate(datetime = as.Date(datetime))

obs_df <- targets_compare_df |>
  mutate(datetime = as.Date(datetime)) |>
  filter(datetime >= min(plot_df$datetime),
         datetime <= max(plot_df$datetime)) |>
  select(datetime, observation)

ggplot(plot_df, aes(x = datetime)) +
  geom_ribbon(aes(ymin = quantile10, ymax = quantile90),
              colour = 'lightblue', fill = 'lightblue') +
  geom_line(aes(y = mean)) +
  geom_point(data = obs_df, aes(x = datetime, y = observation),
             color = 'black', size = 2) +
  scale_y_reverse(limits = c(10, 0)) +
  labs(y = 'DCM Depth (m)') +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 0.2),
        text = element_text(size = 10),
        panel.grid.minor = element_blank())

