#### Function to run DCM depth model forecasts for VERA
## MP: February 2025
## Updated: dynamic covariate sub-models (Schmidt stability, Secchi depth)
## Covariates predicted from weather drivers + day of year instead of held constant
# do the reforecast with this script

#notes with Austin
#drop your driver uncertainty to constrain the model
#just use process uncertainty (CI will remain constant throughout entire forecast horizon)
#or use fewer wind ensemble members (try it with 10 instead maybe)

#use the schmidt stability from the day of output of FLARE (horizon 1)
#train directly on FLARE instead of the targets 
#this is just like NOAA
#SO ALSO DO THIS FOR TEMP

#check to see availability of data for DCM depth

library(tidyverse)
library(arrow)
library(ranger)

#eventual plan to add thermocline depth as well
#(for now just focusing on using secchi, schmidt stability, windspeed, and air temp)


#### helper functions ####

## function to pull current value
current_value <- function(dataframe, variable, start_date){

  value <- dataframe |>
    mutate(datetime = as.Date(datetime)) |>
    filter(datetime == start_date,
           variable == variable) |>
    pull(observation)

  return(value)
}

# function to generate ensemble of DCM depth IC based on standard deviation around current observation
#this is not used
# get_IC_uncert <- function(curr_dcm, n_members, ic_sd = 0.5){
#   rnorm(n = n_members, mean = curr_dcm, sd = ic_sd)
# }


#### main function ####

generate_DCMdepth_forecast <- function(forecast_date,
                                       forecast_horizon,
                                       n_members,
                                       output_folder,
                                       calibration_start_date,
                                       model_id,
                                       targets_url,
                                       var,
                                       site,
                                       forecast_depths = 'focal',
                                       project_id = 'vera4cast') {

  # # ##FOR TESTING REMOVE WHEN DONE####
  if(exists("curr_reference_datetime") == FALSE){

    curr_reference_datetime <- Sys.Date()

  }else{

    print('Running Reforecast')

  }
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

  site <- "fcre"
  output_folder <- paste0("./model_output/fDCMdepth_mp/", model_id, "_", site, "_", forecast_date, ".csv")
  # # ###THINGS TO REMOVE ENDS HERE


  #### Get targets ####
  message('Getting targets')
  #read once and reuse for all target subsets (avoids 4 separate downloads)
  all_targets <- readr::read_csv(targets_url, show_col_types = F) |>
    filter(site_id %in% site,
           datetime <= forecast_date)

  targets <- all_targets |>
    filter(variable %in% var)

  #this function tells me data availability
  source("R/fDCMdepth_mp/data_availability_function.R")

  #what do we have
  # check <- all_targets |>
  #   distinct(variable)

  #### Bathymetry ####
  #needed for schmidt stability calculation
  bath <- read_csv("https://pasta.lternet.edu/package/data/eml/edi/1254/1/f7fa2a06e1229ee75ea39eb586577184")

  bathFCR <- bath|>
    filter(Reservoir == "FCR")

  # interpolate bathymetry to 0.1m resolution
  bth_depths_interp <- seq(min(bathFCR$Depth_m), max(bathFCR$Depth_m), by = 0.1)
  bth_areas_interp  <- approx(x = bathFCR$Depth_m, y = bathFCR$SA_m2,
                              xout = bth_depths_interp, rule = 2)$y

  bth_depths <- bth_depths_interp
  bth_areas  <- bth_areas_interp


  #### FLARE forecast covariates ####
  #pulling FLARE outputs to use as future covariates

  fcre_reforecast <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/forecasts/parquet/site_id=fcre/model_id=glm_aed_flare_v3/"),
                                      endpoint_override = 'amnh1.osn.mghpcc.org',
                                      anonymous = TRUE)

  ####variable options from flare####
  #(exploratory only - skipping to save time)
  # flare_variables <- arrow::open_dataset(fcre_reforecast) |>
  #   filter(reference_datetime > ymd("2026-01-01"))|>
  #   distinct(variable)|>
  #   collect()
  #variables are
  #Temp_C_mean (no schmidt stability or thermocline depth- would have to calculate using bathymetry)
  #secchi

  # open the dataset once and reuse
  flare_ds1 <- arrow::open_dataset(fcre_reforecast)
  #start anytime after january 2021
  flare_ds <- flare_ds1|>
    mutate(reference_datetime = as_date(reference_datetime))|>
    filter(reference_datetime >= ymd("2021-01-05"))

  #pull FLARE forecast issued on/just before forecast_date so the future
  #covariates actually align with the forecast horizon
  forecast_horizon_start <- as.POSIXct(forecast_date + 1, tz = "UTC")
  forecast_ref_cutoff    <- as.POSIXct(forecast_date + 1, tz = "UTC")

  #future water temp####
  flare_ref_temp <- flare_ds |>
    filter(variable == "Temp_C_mean",
           reference_datetime <= forecast_ref_cutoff) |>
    summarise(max_ref = max(reference_datetime)) |>
    collect() |>
    pull(max_ref)

  df_flare_new <- flare_ds |>
    filter(
      variable == "Temp_C_mean",
      parameter <= 31,
      reference_datetime == flare_ref_temp,
      datetime >= forecast_horizon_start
    ) |>
    collect()

  #not touched
  df_flare_new_forbind <- df_flare_new |>
    rename(datetime_date = datetime) |>
    filter(parameter <= 31) |>
    select(-reference_date) |>
    mutate(variable = "temperature",
           reference_datetime = as.character(reference_datetime),
           site_id = site,
           model_id = 'test_runs3') |>
    mutate(parameter = as.numeric(parameter)) |>
    select(reference_datetime, datetime_date, site_id, depth, family, parameter, variable, prediction, model_id)

  ####future secchi####
  flare_ref_secchi <- flare_ds |>
    filter(variable == "secchi",
           reference_datetime <= forecast_ref_cutoff) |>
    summarise(max_ref = max(reference_datetime)) |>
    collect() |>
    pull(max_ref)

  #could also just filter on horizon
  secchi_flare_new <- flare_ds |>
    filter(
      variable == "secchi",
      parameter <= 31,
      reference_datetime == flare_ref_secchi,
      datetime >= forecast_horizon_start
    ) |>
    collect()

  secchi_flare_new_forbind <- secchi_flare_new |>
    rename(datetime_date = datetime) |>
    filter(parameter <= 31) |>
    select(-reference_date) |>
    mutate(variable = "secchi",
           reference_datetime = as.character(reference_datetime),
           site_id = site,
           model_id = 'test_runs3') |>
    mutate(parameter = as.numeric(parameter)) |>
    select(reference_datetime, datetime_date, site_id, depth, family, parameter, variable, prediction, model_id)

  #secchi is calculated from light extinction
  #extc - light extinction outputted from FLARE

  # future Schmidt stability per ensemble member from FLARE water temp profiles
  # uses bathymetry and temp-at-depth to compute daily Schmidt stability
  future_schmidt_ens <- df_flare_new_forbind |>
    group_by(datetime_date, parameter) |>
    arrange(depth) |>
    summarise(
      SchmidtStability_Jm2_mean = rLakeAnalyzer::schmidt.stability(
        wtr = prediction, depths = depth,
        bthA = bth_areas, bthD = bth_depths
      ),
      .groups = "drop"
    ) |>
    mutate(datetime = as.Date(datetime_date)) |>
    select(datetime, parameter, SchmidtStability_Jm2_mean)

  # future secchi per ensemble member from FLARE
  future_secchi_ens <- secchi_flare_new_forbind |>
    group_by(datetime_date, parameter) |>
    summarise(Secchi_m_sample = mean(prediction, na.rm = TRUE), .groups = "drop") |>
    mutate(datetime = as.Date(datetime_date)) |>
    select(datetime, parameter, Secchi_m_sample)

  # combined future covariates
  future_covariates <- future_schmidt_ens |>
    full_join(future_secchi_ens, by = c("datetime", "parameter"))


  #### Historic covariate####
  #eventually thermocline depth
  #Schmidt stability
  #secchi
  #average air temperature (from NOAA)
  #average windspeed (from NOAA)

  ####historic water temp#### not including this for now
  # historic_temp_profiles <- readr::read_csv(targets_url, show_col_types = F) |>
  #   filter(site_id %in% site,
  #          datetime <= forecast_date,
  #          variable == "Temp_C_mean")
  # temp_pivot <- historic_temp_profiles|>
  #   pivot_wider(names_from = variable, values_from = observation)|>
  #   mutate(Date = as.Date(datetime))
  # data_availability(temp_pivot, "Temp_C_mean")
  # bathymetry data for if I want to make other calculations
  #  #bathymetry data:
  #bath <- read_csv("https://pasta.lternet.edu/package/data/eml/edi/1254/1/f7fa2a06e1229ee75ea39eb586577184")

  ####historic secchi####
  #REDOING HISTORIC SECCHI AND TEMP SO THAT IT'S COMING FROM FLARE####
  #historic water temp####
  df_flare_new <- flare_ds |>
    filter(
      variable == "Temp_C_mean",
      parameter <= 31,
      reference_datetime >= ymd("2021-01-05"),
      reference_datetime <= forecast_date,
      reference_datetime == datetime
    ) |>
    collect()
  
  #not touched
  df_flare_new_forbind <- df_flare_new |>
    rename(datetime_date = datetime) |>
    filter(parameter <= 31) |>
    select(-reference_date) |>
    mutate(variable = "temperature",
           reference_datetime = as.character(reference_datetime),
           site_id = site,
           model_id = 'test_runs3') |>
    mutate(parameter = as.numeric(parameter)) |>
    select(reference_datetime, datetime_date, site_id, depth, family, parameter, variable, prediction, model_id)
  
  ####historic secchi####
  
  secchi_flare_new <- flare_ds |>
    filter(
      variable == "secchi",
      parameter <= 31,
      reference_datetime >= ymd("2021-01-05"),
      reference_datetime <= forecast_date,
      reference_datetime == datetime
    ) |>
    collect()
  
  secchi_flare_new_forbind <- secchi_flare_new |>
    rename(datetime_date = datetime) |>
    filter(parameter <= 31) |>
    select(-reference_date) |>
    mutate(variable = "secchi",
           reference_datetime = as.character(reference_datetime),
           site_id = site,
           model_id = 'test_runs3') |>
    mutate(parameter = as.numeric(parameter)) |>
    select(reference_datetime, datetime_date, site_id, depth, family, parameter, variable, prediction, model_id)
  
  # future Schmidt stability per ensemble member from FLARE water temp profiles
  # uses bathymetry and temp-at-depth to compute daily Schmidt stability
  historic_schmidt_ens <- df_flare_new_forbind |>
    group_by(datetime_date, parameter) |>
    arrange(depth) |>
    summarise(
      SchmidtStability_Jm2_mean = rLakeAnalyzer::schmidt.stability(
        wtr = prediction, depths = depth,
        bthA = bth_areas, bthD = bth_depths
      ),
      .groups = "drop"
    ) |>
    mutate(datetime = as.Date(datetime_date)) |>
    select(datetime, parameter, SchmidtStability_Jm2_mean)|>
    group_by(datetime)|>
    summarise(SchmidtStability_Jm2_mean = mean(SchmidtStability_Jm2_mean, na.rm = TRUE), .groups = "drop")
    
    
  
  # future secchi per ensemble member from FLARE
  historic_secchi_ens <- secchi_flare_new_forbind |>
    group_by(datetime_date, parameter) |>
    summarise(Secchi_m_sample = mean(prediction, na.rm = TRUE), .groups = "drop") |>
    mutate(datetime = as.Date(datetime_date)) |>
    select(datetime, parameter, Secchi_m_sample)|>
    group_by(datetime)|>
    summarise(Secchi_m_sample = mean(Secchi_m_sample, na.rm = TRUE), .groups = "drop")

  
  # combined future covariates
  historic_covariates <- historic_schmidt_ens |>
    full_join(historic_secchi_ens, by = c("datetime"))

  covariate_vars <- c("SchmidtStability_Jm2_mean", "Secchi_m_sample")

  

  #### Weather drivers ####
  #forecasted weather data
  # Get the forecasted weather data (shortwave, air temperature, wind components)
  message('Getting weather')

  met_vars <- c("surface_downwelling_shortwave_flux_in_air", "air_temperature",
                "eastward_wind", "northward_wind")

  noaa_date <- forecast_date
  print(paste0('NOAA data from: ', noaa_date))

  met_s3_future <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/drivers/met/gefs-v12/stage2",
                                              paste0("reference_datetime=", noaa_date),
                                              paste0("site_id=", site)),
                                    endpoint_override = 'amnh1.osn.mghpcc.org',
                                    anonymous = TRUE)

  forecast_weather_raw <- arrow::open_dataset(met_s3_future) |>
    dplyr::filter(variable %in% met_vars) |>
    mutate(datetime_date = as.Date(datetime),
           reference_datetime = forecast_date) |>
    group_by(reference_datetime, datetime_date, variable, parameter) |>
    summarise(prediction = mean(prediction, na.rm = T), .groups = "drop") |>
    mutate(parameter = parameter + 1) |>
    dplyr::collect()

  # compute wind speed from eastward and northward components per ensemble member per day
  wind_components <- forecast_weather_raw |>
    filter(variable %in% c("eastward_wind", "northward_wind")) |>
    pivot_wider(names_from = variable, values_from = prediction) |>
    mutate(variable = "wind_speed",
           prediction = sqrt(eastward_wind^2 + northward_wind^2)) |>
    select(reference_datetime, datetime_date, variable, parameter, prediction)

  forecast_weather <- forecast_weather_raw |>
    filter(!variable %in% c("eastward_wind", "northward_wind")) |>
    bind_rows(wind_components)

  # historic weather
  historic_noaa_s3 <- arrow::s3_bucket(paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3/site_id=", site),
                                       endpoint_override = "amnh1.osn.mghpcc.org",
                                       anonymous = TRUE)

  #push the aggregation into arrow and bound by calibration window so we don't
  #drag the entire stage3 archive across the network
  historic_weather_raw <- arrow::open_dataset(historic_noaa_s3) |>
    filter(datetime < forecast_date,
           datetime >= as.Date(calibration_start_date),
           variable %in% met_vars) |>
    group_by(datetime, variable) |>
    summarise(prediction = mean(prediction, na.rm = T), .groups = "drop") |>
    collect()

  # compute historic wind speed from components
  historic_wind <- historic_weather_raw |>
    filter(variable %in% c("eastward_wind", "northward_wind")) |>
    pivot_wider(names_from = variable, values_from = prediction) |>
    mutate(variable = "wind_speed",
           prediction = sqrt(eastward_wind^2 + northward_wind^2)) |>
    select(datetime, variable, prediction)

  historic_weather <- historic_weather_raw |>
    filter(!variable %in% c("eastward_wind", "northward_wind")) |>
    bind_rows(historic_wind) |>
    pivot_wider(names_from = variable, values_from = prediction) |>
    mutate(datetime = as.Date(datetime))|>
    group_by(datetime)|>
    summarise(air_temperature = mean(air_temperature, na.rm = T),
              wind_speed = mean(wind_speed, na.rm = T), .groups = "drop")


  #### Fit random forest model for DCM depth ####
  message('Preparing training data')

  # predictors
  all_predictor_vars <- c("SchmidtStability_Jm2_mean", "Secchi_m_sample",
                          "air_temperature", "wind_speed",
                          "doy_sin", "doy_cos")

  fit_df <- targets |>
    mutate(datetime = as.Date(datetime)) |>
    filter(datetime < forecast_date,
           datetime >= calibration_start_date) |>
    group_by(datetime, variable) |>
    summarise(observation = mean(observation, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = variable, values_from = observation) |>
    full_join(historic_covariates, by = "datetime") |>
    full_join(historic_weather, by = "datetime") |>
    mutate(doy_sin = sin(2 * pi * lubridate::yday(datetime) / 365),
           doy_cos = cos(2 * pi * lubridate::yday(datetime) / 365))|>
    arrange(datetime)|>
    filter(!is.na(ChlorophyllMaximum_depth_sample))
  
  # only keep predictors that actually exist in the data for this site
  predictor_vars <- all_predictor_vars[all_predictor_vars %in% names(fit_df)]
  message(paste0('Using predictors: ', paste(predictor_vars, collapse = ", ")))

  # remove NAs for ranger (cannot handle missing values)
  fit_df_noNA <- fit_df |>
    select(all_of(c(var, predictor_vars))) |>
    na.omit()

  #I technically only have 37 right now since 2024-10-01 is earliest FLARE output for temp and secchi
  
  message('Fitting random forest model')

  # fit ranger with quantile regression enabled for uncertainty estimation
  rf_formula <- reformulate(predictor_vars, response = var)

  #taking this straight from my DCM depth project 
  grid_trees <- c(100, 200, 300, 500)
  grid_nodes <- c(2, 4, 6, 8)
  max_mtry  <- c(2,3,4,5,6)
  grid_mtry <- c(1,2,3,4,5,6)
  
  results <- list()
  idx <- 1
  y_obs <- fit_df_noNA[[var]]
  library(randomForest)
  
  for (nt in grid_trees) {
    for (ns in grid_nodes) {
      for (mt in grid_mtry) {
        fit <- randomForest(
          formula    = rf_formula,
          data       = fit_df_noNA,
          ntree      = nt,
          mtry       = mt,
          nodesize   = ns,
          importance = TRUE
        )
        rsq <- fit$rsq[length(fit$rsq)]
        mse <- fit$mse[length(fit$mse)]
        
        results[[idx]] <- data.frame(
          Trees = nt,
          NodeSize = ns,
          mtry = mt,
          OOB_R2 = rsq,
          OOB_MSE = mse,
          OOB_RMSE = sqrt(mse)
        )
        idx <- idx + 1
      }
    }
  }
  
  RF_tuning_scores <- dplyr::bind_rows(results) %>%
    arrange(desc(OOB_R2), OOB_MSE)
  print(RF_tuning_scores)
  
  best <- RF_tuning_scores[1, ]
  
  dcm_model <- randomForest(
    formula    = rf_formula,
    data       = fit_df_noNA,
    ntree      = best$Trees,
    mtry       = best$mtry,
    nodesize   = best$NodeSize,
    importance = TRUE
  )
  
 
  # get process uncertainty from OOB residuals
  dcm_model
  
  oob_preds <- dcm_model$predictions
  oob_residuals <- oob_preds - fit_df_noNA[[var]]
  sigma <- sd(oob_residuals, na.rm = TRUE)

  message(paste0('Random forest OOB R-squared: ', round(dcm_model$r.squared, 3)))
  message(paste0('Process uncertainty (sigma): ', round(sigma, 3)))
  #COME BACK IN AND ACTUALLY ADD IN OOB RESIDUALS
  #ADD VARIANCE, OOB IS CURRENTLY NOT ACTUALLY ADDING ANYTHING


  #### Generate forecast ####
  message('Make forecast dataframe')

  forecasted_dates <- seq(from = ymd(forecast_date) + 1, to = ymd(forecast_date) + forecast_horizon, by = "day")

  # set up table to hold forecast output
  forecast_full_unc <- tibble(date = rep(forecasted_dates, times = n_members),
                              ensemble_member = rep(1:n_members, each = length(forecasted_dates)),
                              reference_datetime = forecast_date,
                              Horizon = date - reference_datetime,
                              forecast_variable = var,
                              value = as.double(NA),
                              uc_type = "total")

  #-------------------------------------

  message('Generating forecast')
  print(paste0('Running forecast starting on: ', forecast_date))

  
  #CHECK IN ON THIS 
  # define quantile levels for ensemble members (evenly spaced)
  quantile_levels <- seq(0.01, 0.99, length.out = n_members)

  for(i in 1:length(forecasted_dates)) {

    # pull prediction dataframe for the relevant date
    dcm_pred <- forecast_full_unc |>
      filter(date == forecasted_dates[i])

    # pull NOAA weather ensembles for this date
    met_airtemp_driv <- forecast_weather |>
      filter(variable == "air_temperature",
             ymd(reference_datetime) == forecast_date,
             ymd(datetime_date) == forecasted_dates[i])

    met_wind_driv <- forecast_weather |>
      filter(variable == "wind_speed",
             ymd(reference_datetime) == forecast_date,
             ymd(datetime_date) == forecasted_dates[i])

    # pull FLARE covariates for this date
    flare_cov_day <- future_covariates |>
      filter(datetime == forecasted_dates[i])

    # join all drivers by ensemble member (parameter)
    new_data <- met_airtemp_driv |>
      select(parameter, air_temperature = prediction) |>
      left_join(met_wind_driv |> select(parameter, wind_speed = prediction), by = "parameter") |>
      left_join(flare_cov_day |> select(parameter, SchmidtStability_Jm2_mean, Secchi_m_sample), by = "parameter") |>
      mutate(doy_sin = sin(2 * pi * lubridate::yday(forecasted_dates[i]) / 365),
             doy_cos = cos(2 * pi * lubridate::yday(forecasted_dates[i]) / 365)) |>
      select(-parameter)

    #ranger errors on NA inputs - skip days where any predictor is missing
    #(this can happen if FLARE doesn't extend that far into the horizon)
    if(any(is.na(new_data[, predictor_vars]))){
      message(paste0('Skipping ', forecasted_dates[i], ' - missing predictor values'))
      next
    }

    # predict using quantile regression forest
    # each ensemble member gets a different quantile of the conditional distribution
    rf_pred <- predict(dcm_model,
                       data = new_data,
                       type = "quantiles",
                       quantiles = quantile_levels)
    #LOOK INTO THIS AND MAYBE SUBMITTING MEAN AND STANDARD DEVIATION

    # extract the diagonal: member j gets quantile j from its own input row
    #quantile RF already gives the conditional spread across members so we don't
    #also add rnorm(sigma) - was double-counting process + structural uncertainty
    dcm_pred$value <- sapply(1:n_members, function(j) rf_pred$predictions[j, j])

    # insert values back into the forecast dataframe
    forecast_full_unc <- forecast_full_unc |>
      rows_update(dcm_pred, by = c("date", "ensemble_member", "forecast_variable", "uc_type"))

  } 


  #### Format output to vera standard ####
  forecast_df <- forecast_full_unc |>
    rename(datetime = date,
           variable = forecast_variable,
           prediction = value,
           parameter = ensemble_member) |>
    mutate(family = 'ensemble',
           duration = "P1D",
           depth_m = ifelse(forecast_depths == 'focal', NA_real_, as.numeric(forecast_depths)),
           project_id = project_id,
           model_id = model_id,
           site_id = site) |>
    select(datetime, reference_datetime, model_id, site_id,
           parameter, family, prediction, variable, depth_m,
           duration, project_id)

  return(forecast_df)

} ##### end function
