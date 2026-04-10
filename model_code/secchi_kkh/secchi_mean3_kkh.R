secchi_forecast <- function(forecast_start,
                            se_period,
                            se_start_date = forecast_start - se_period,
                            weeks = 4,
                            mean_obs = 3,
                            sites = c("fcre", "bvre"),
                            forecast_variable = "Secchi_m_sample",
                            targets_url = "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz") {
  
  library(dplyr)
  library(readr)
  library(lubridate)
  library(tidyr)
  
  all_sites_output <- vector("list", length(sites))
  
  for (i in sites) {
    site <- i
    print(site)

  #-----------------------------
  # LOAD DATA
  #-----------------------------
  targets <- read_csv(targets_url, show_col_types = FALSE) %>%
    filter(variable == forecast_variable,
           site_id == site) %>%
    arrange(datetime) %>%
    select(datetime, observation)
  
  #-----------------------------
  # STEP 1: COMPUTE SE (HISTORICAL)
  #-----------------------------
  results_list <- vector("list", se_period)
  
  for (day in seq_len(se_period)) {
    
    forecast_date <- se_start_date + days(day)
    
    idx <- which(targets$datetime < forecast_date)
    if (length(idx) < mean_obs) next
    
    current_obs <- targets$observation[idx]
    buffer <- tail(na.omit(current_obs), mean_obs)
    
    day_results <- vector("list", weeks)
    
    for (week in seq_len(weeks)) {
      
      mu <- mean(buffer)
      
      horizon_dates <- seq(
        from = forecast_date + (week - 1) * 7,
        by = "day",
        length.out = 7
      )
      
      df <- data.frame(
        date = horizon_dates,
        prediction = rep(mu, 7),
        week = week,
        forecast_date = forecast_date
      )
      
      day_results[[week]] <- df
      
      # recursive update
      buffer <- c(tail(buffer, mean_obs - 1), mu)
    }
    
    results_list[[day]] <- bind_rows(day_results)
  }
  
  reforecast <- bind_rows(results_list)
  
  combined <- reforecast %>%
    left_join(targets, by = c("date" = "datetime")) %>%
    drop_na(observation)
  
  # residual-based SE by week
  se_combined <- combined %>%
    group_by(week) %>%
    summarise(
      se = sd(prediction - observation),
      .groups = "drop"
    )
  
  #-----------------------------
  # STEP 2: FINAL FORECAST AT forecast_start
  #-----------------------------
  
  idx <- which(targets$datetime < forecast_start)
  current_obs <- targets$observation[idx]
  buffer <- tail(na.omit(current_obs), mean_obs)
  
  final_results <- vector("list", weeks)
  
  for (week in seq_len(weeks)) {
    
    mu1 <- mean(buffer)
    mu <- pmax(mu1, 0)
    
    
    sigma <- se_combined$se[se_combined$week == week]
    
    if ((mu - sigma) < 0) {
      sigma <- mu
    }
    
    horizon_dates <- seq(
      from = forecast_start + (week - 1) * 7,
      by = "day",
      length.out = 7
    )
    
    df <- data.frame(
      date = rep(horizon_dates, 2),
      parameter = rep(c("mu", "sigma"), each = 7),
      prediction = c(rep(mu, 7), rep(sigma, 7)),
      week = week,
      forecast_date = forecast_start
    )
    
    final_results[[week]] <- df
    
    # recursive update
    buffer <- c(tail(buffer, mean_obs - 1), mu)
  }
  
  forecast_output <- bind_rows(final_results)
  
  forecast_output <- bind_rows(final_results) %>%
    mutate(
      reference_datetime = forecast_date,
      datetime = date,
      site_id = site,
      model_id = "secchi_last3obs_mean",   # you can change this
      variable = forecast_variable,
      family = "normal",             # since you're using mu/sigma
      depth_m = NA_real_,
      project_id = "vera4cast",
      duration = "P1D"
    ) %>%
    select(reference_datetime,
           datetime,
           site_id,
           model_id,
           variable,
           family,
           parameter,
           prediction,
           depth_m,
           project_id,
           duration)
  
  all_sites_output[[i]] <- forecast_output
  
  }

  return(bind_rows(all_sites_output))
  
  # ## validate and submit forecast
  
  # validate
  # print('Validating File...')
  # vera4castHelpers::forecast_output_validator(all_sites_output)
  # vera4castHelpers::submit(forecast_file_abs_path, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)
  # 
}

forecast_output <- secchi_forecast(forecast_start = as.Date("2026-03-01"),
                                   se_period = 730,
                                   weeks = 4, 
                                   mean_obs = 3,
                                   sites = c("fcre", "bvre"))

vera4castHelpers::forecast_output_validator(forecast_output)
vera4castHelpers::submit(forecast_output, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)
