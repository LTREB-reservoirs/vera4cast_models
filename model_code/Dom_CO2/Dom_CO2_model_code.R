# ------ Load packages -----
# remotes::install_github("LTREB-reservoirs/vera4castHelpers")
library(tidyverse)
library(lubridate)
library(vera4castHelpers)
#--------------------------#

##start function

Dom_CO2model_function <- function(forecast_rundate,
                                  forecast_model_id,
                                  forecast_target){
  
  
  
  my_model_id <- forecast_model_id
  focal_sites <- c('fcre')
  
  target_variable <- forecast_target
  covariate_variables <- c("Chla_ugL_mean")
  
  #------- 1. Read Target Data --------
  targets_url <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"
  targets <- read_csv(targets_url, show_col_types = FALSE) |>
    filter(site_id %in% focal_sites) |>
    # Force to a clean Date (removes 00:00:00 times that mess up joins)
    mutate(datetime = as_date(datetime))
  #------------------------------------#
  
  
  
  # ------ 2. Weather Data Ingestion ------
  met_variables <- c("air_temperature")
  
  # Past weather
  weather_past_s3 <- vera4castHelpers::noaa_stage3()
  weather_past <- weather_past_s3 |>
    dplyr::filter(site_id %in% focal_sites,
                  datetime >= ymd('2018-01-01'),
                  variable %in% met_variables) |>
    dplyr::collect()
  
  weather_past_daily <- weather_past |>
    mutate(datetime = as_date(datetime)) |>
    group_by(datetime, site_id, variable) |>
    summarize(prediction = mean(prediction, na.rm = TRUE), .groups = "drop") |>
    mutate(prediction = ifelse(variable == "air_temperature", prediction - 273.15, prediction)) |>
    pivot_wider(names_from = variable, values_from = prediction)
  
  # Future weather
  forecast_date <- forecast_rundate #Sys.Date()
  noaa_date <- forecast_date - days(1)
  
  weather_future_s3 <- vera4castHelpers::noaa_stage2(start_date = as.character(noaa_date))
  weather_future <- weather_future_s3 |>
    dplyr::filter(datetime >= forecast_date,
                  site_id %in% focal_sites,
                  variable %in% met_variables) |>
    collect()
  
  if(nrow(weather_future) == 0) {
    noaa_date <- forecast_date - days(2)
    weather_future_s3 <- vera4castHelpers::noaa_stage2(start_date = as.character(noaa_date))
    weather_future <- weather_future_s3 |>
      dplyr::filter(datetime >= forecast_date,
                    site_id %in% focal_sites,
                    variable %in% met_variables) |>
      collect()
  }
  
  weather_future_daily <- weather_future |>
    mutate(datetime = as_date(datetime)) |>
    group_by(datetime, site_id, parameter, variable) |>
    summarize(prediction = mean(prediction, na.rm = TRUE), .groups = "drop") |>
    mutate(prediction = ifelse(variable == "air_temperature", prediction - 273.15, prediction)) |>
    pivot_wider(names_from = variable, values_from = prediction) |>
    select(any_of(c('datetime', 'site_id', met_variables, 'parameter')))
  #--------------------------------------#
  
  
  
  # --- 3. Extract, Merge, & Forward-Fill Covariates---
  weather_combined <- bind_rows(
    weather_past_daily,
    weather_future_daily |>
      select(datetime, site_id, any_of("air_temperature")) |>
      group_by(datetime, site_id) |>
      summarize(air_temperature = mean(air_temperature, na.rm = TRUE), .groups = 'drop')
  ) |>
    distinct(datetime, site_id, .keep_all = TRUE) |>
    arrange(site_id, datetime)
  
  # Extract Chla
  covariates_historical <- targets |>
    filter(variable %in% covariate_variables, depth_m == 1.6) |>
    select(datetime, site_id, variable, observation) |>
    group_by(datetime, site_id, variable) |>
    summarize(observation = mean(observation, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = variable, values_from = observation)
  
  # Merge weather and Chla
  model_drivers <- weather_combined |>
    left_join(covariates_historical, by = c("datetime", "site_id")) |>
    distinct(datetime, site_id, .keep_all = TRUE) |>
    group_by(site_id) |>
    arrange(datetime) |>
    tidyr::fill(all_of(covariate_variables), .direction = "down") |>
    ungroup()
  
  # Prepare Target CO2 Data
  targets_co2 <- targets |>
    filter(variable == target_variable, depth_m == 0.1) |>
    group_by(datetime, site_id) |>
    summarize(co2 = mean(observation, na.rm = TRUE), .groups = "drop")
  
  # Merge targets with drivers
  targets_lm <- targets_co2 |>
    left_join(model_drivers, by = c("datetime", "site_id")) |>
    arrange(site_id, datetime) |>
    group_by(site_id) |>
    mutate(co2_yday = lag(co2, 1)) |>
    ungroup() |>
    filter(complete.cases(co2, co2_yday, air_temperature, Chla_ugL_mean))
  #----------------------------------------------------#
  
  
  
  # ----- 4. Fit model & generate forecast----
  forecast_df <- NULL
  n_members <- 31 #
  
  for(i in 1:length(focal_sites)) {
    
    curr_site <- focal_sites[i]
    site_target <- targets_lm |> filter(site_id == curr_site)
    
    noaa_future_site <- weather_future_daily |>
      filter(site_id == curr_site) |>
      left_join(model_drivers |> select(datetime, site_id, all_of(covariate_variables)),
                by = c("datetime", "site_id")) |>
      distinct(datetime, site_id, parameter, .keep_all = TRUE)
    
    fit <- lm(co2 ~ co2_yday + air_temperature + Chla_ugL_mean, data = site_target)
    fit_summary <- summary(fit)
    
    coeffs <- fit$coefficients
    params_se <- fit_summary$coefficients[, 2]
    sigma <- sd(fit$residuals, na.rm = TRUE)
    
    forecast_start_date <- forecast_date
    forecasted_dates <- seq(from = forecast_start_date, to = max(noaa_future_site$datetime), by = "day")
    
    curr_co2 <- tail(site_target$co2, 1)
    ic_uc <- rnorm(n = n_members, mean = curr_co2, sd = 0.2)
    
    ic_df <- tibble(
      forecast_date = rep(forecast_start_date, times = n_members),
      ensemble_member = 1:n_members,
      forecast_variable = target_variable,
      value = ic_uc,
      uc_type = "total"
    )
    
    param_df <- data.frame(
      beta1 = rnorm(n_members, coeffs[1], params_se[1]),
      beta2 = rnorm(n_members, coeffs[2], params_se[2]),
      beta3 = rnorm(n_members, coeffs[3], params_se[3]),
      beta4 = rnorm(n_members, coeffs[4], params_se[4])
    )
    
    forecast_total_unc <- tibble(
      forecast_date = rep(forecasted_dates, times = n_members),
      ensemble_member = rep(1:n_members, each = length(forecasted_dates)),
      forecast_variable = target_variable,
      value = as.double(NA),
      uc_type = "total"
    ) |>
      rows_update(ic_df, by = c("forecast_date","ensemble_member","forecast_variable","uc_type"))
    
    # --- FORECAST GENERATION ---
    for(d in 2:length(forecasted_dates)) {
      
      temp_pred <- forecast_total_unc |> filter(forecast_date == forecasted_dates[d])
      temp_lag <- forecast_total_unc |> filter(forecast_date == forecasted_dates[d-1])
      temp_driv <- noaa_future_site |> filter(datetime == forecasted_dates[d])
      
      # Expand drivers to match 31 members
      temp_driv_31 <- temp_driv[rep(1:nrow(temp_driv), length.out = n_members), ]
      
      temp_pred$value <- param_df$beta1 +
        temp_lag$value * param_df$beta2 +
        temp_driv_31$air_temperature * param_df$beta3 +
        temp_driv_31$Chla_ugL_mean * param_df$beta4 +
        rnorm(n = n_members, mean = 0, sd = sigma)
      
      forecast_total_unc <- forecast_total_unc |>
        rows_update(temp_pred, by = c("forecast_date","ensemble_member","forecast_variable","uc_type"))
    }
    
    curr_site_df <- forecast_total_unc |>
      filter(forecast_date > forecast_start_date) |>
      rename(datetime = forecast_date, parameter = ensemble_member, prediction = value) |>
      mutate(site_id = curr_site, variable = target_variable, depth_m = 0.1) |>
      select(datetime, site_id, depth_m, parameter, prediction, variable)
    
    forecast_df <- dplyr::bind_rows(forecast_df, curr_site_df)
  }
  #--------------------------------------#
  
  #---- 5. Format to VERA standard ----
  forecast_df_EFI <- forecast_df %>%
    mutate(model_id = my_model_id,
           reference_datetime = forecast_date,
           family = 'ensemble',
           duration = 'P1D',
           parameter = as.character(parameter),
           project_id = 'vera4cast') %>%
    select(project_id, model_id, datetime, reference_datetime, duration, site_id, depth_m, family, parameter, variable, prediction)
  
  return(forecast_df_EFI)
  
} ## end of function


## VIZ CODE RUN OUTSIDE OF FUNCTION BELOW

# --- 6. Model Diagnostics and Forecast Visualization ---

# # 1. Linear Model Summary
# message("--- Linear Model Summary ---")
# print(summary(fit))
#
# # 2. Base R Diagnostic Plots for the Linear Model
# # (If it pauses in the console, press Enter to tab through the plots)
# par(mfrow = c(2, 2))
# plot(fit)
# par(mfrow = c(1, 1))
#
# # 3. Forecast Visualization
# forecast_plot <- forecast_df_EFI |>
#   ggplot(aes(x = as_date(datetime), y = prediction, group = parameter)) +
#   geom_line(alpha = 0.15, color = "dodgerblue") + # Adjusted alpha slightly for 31 members
#   facet_wrap(~site_id) +
#   theme_bw() +
#   labs(
#     title = paste0('Forecast generated for ', forecast_df_EFI$variable[1]),
#     subtitle = paste0('Reference Date: ', forecast_df_EFI$reference_datetime[1]),
#     x = "Date",
#     y = expression(Predicted~CO[2]~(mu*mol/L))
#   ) +
#   theme(
#     plot.title = element_text(face = "bold", size = 14),
#     axis.text = element_text(size = 10)
#   )
#
# print(forecast_plot)
#
# # Save the plot
# plot_file_name <- paste0(forecast_df_EFI$variable[1], '-', forecast_df_EFI$reference_datetime[1], ".png")
# ggsave(plot_file_name, plot = forecast_plot, width = 8, height = 5, bg = "white")
# message("Forecast plot saved as: ", plot_file_name)
# #----------------------------------------------------#
