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

#lowest observation is 2018-10-22 

# ---- Generate the forecasts -----
forecast_date <- as.Date('2018-10-20') ## could call Sys.Date() here to run true forecast
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
  filter(variable == target_variable, site_id %in% c("fcre"))

#depth_m is NA in both targets and forecast for this variable, but joins don't
#match on NA - assign a placeholder so the score join actually attaches obs
targets_compare_df <- targets_compare_df |> mutate(depth_m = 1.6)
reforecast_df      <- reforecast_df      |> mutate(depth_m = 1.6)

reforecast_score_df <- generate_forecast_score(targets_df = targets_compare_df,
                                               forecast_df = reforecast_df)


#join observations onto the score df directly so they actually show on the plot
#(the score4cast join sometimes drops obs for this variable)
obs_df <- targets_compare_df |>
  mutate(datetime = as.Date(datetime)) |>
  select(datetime, observation)

plot_df <- reforecast_score_df |>
  mutate(datetime = as.Date(datetime)) |>
  select(-any_of("observation")) |>
  left_join(obs_df, by = "datetime")

ggplot(plot_df, aes(x = datetime)) +
  geom_ribbon(aes(ymin = quantile10, ymax = quantile90),
              colour = 'lightblue', fill = 'lightblue') +
  geom_line(aes(y = mean)) +
  geom_point(aes(y = observation), color = 'black') +
  scale_y_reverse(limits = c(10, 0)) +
  labs(y = 'DCM Depth (m)') +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 0.2),
        text = element_text(size = 10),
        panel.grid.minor = element_blank())

