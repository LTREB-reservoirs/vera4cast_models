#### Function to run DCM depth model forecasts for VERA
## MP: February 2025
## Updated: dynamic covariate sub-models (thermocline depth, Schmidt stability, Secchi depth)
## Covariates predicted from weather drivers + day of year instead of held constant
#do the reforecast with this script

#notes with Austin
#drop your driver uncertainty to constrain the model
#just use process uncertainty (CI will remain constant throughout entire forecast horizon)
#or use fewer wind ensemble members (try it with 10 instead maybe)


library(tidyverse)
library(arrow)
library(ranger)

#eventual plan to add thermocline depth as well
#(for now just focusing on using secchi, schmidt stability, windspeed, and air temp)

## helper functions

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
get_IC_uncert <- function(curr_dcm, n_members, ic_sd = 0.5){
  rnorm(n = n_members, mean = curr_dcm, sd = ic_sd)
}


## main function

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
  
  # ##FOR TESTING REMOVE WHEN DONE####
  #function to pull current value
  current_value <- function(dataframe, variable, start_date){

    value <- dataframe |>
      mutate(datetime = as.Date(datetime)) |>
      filter(datetime == start_date,
             variable == variable) |>
      pull(observation)

    return(value)
  }

  # function to generate ensemble of DCM depth IC based on standard deviation around current observation
  get_IC_uncert <- function(curr_dcm, n_members, ic_sd = 0.5){
    rnorm(n = n_members, mean = curr_dcm, sd = ic_sd)
  }

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
  forecast_date <- forecast_date
  forecast_horizon <- forecast_horizon
  n_members <-  n_members
  output_folder <- output_folder
  model_id <-  model_id
  targets_url<-  targets_url
  var <- var
  forecast_depths <-  forecast_depths
  project_id <-  project_id
  calibration_start_date <-  calibration_start_date

  # ###THINGS TO REMOVE ENDS HERE

  # Get targets####
  message('Getting targets')
  targets <- readr::read_csv(targets_url, show_col_types = F) |>
    filter(variable %in% var,
           site_id %in% site,
           datetime <= forecast_date)
  
  
####FLARE####
  
  #probably won't need this
  #focus on training on the last year or two
  #I just took this from Dexter but this will not work
  # water_temp_4cast_old_url <- "bio230121-bucket01/vt_backup/forecasts/parquet/"
  # 
  # #this already selects for the site I want 
  # backup_forecasts <- arrow::s3_bucket(paste0(water_temp_4cast_old_url,'site_id=',site,'/model_id=test_runS3/'),
  #                                      endpoint_override = 'amnh1.osn.mghpcc.org',
  #                                      anonymous = TRUE)
  #all the variable options from flare
  #flare_variables <- arrow::open_dataset(backup_forecasts) |>
  # distinct(variable) |>
  # collect()
  
  #see all column names
  #flarecols <- names(arrow::open_dataset(backup_forecasts))
  
  #i want temp for schmidt stability and secchi as a proxy for PZ
  #i don't think I need this for historical right?
  # df_flare_old <- arrow::open_dataset(backup_forecasts) |>
  #   filter(variable %in% c("temperature"),
  #          #model_id == "test_runS3", #other models for FCR, this is the only one for BVR in backups bucket
  #          parameter <= 31,
  #          reference_datetime > "2022-11-07 00:00:00") |>
  #   dplyr::collect()
  # #if you don't want to reload just read in saved CSV
  # df_flare_old_forbind <- df_flare_old |> 
  #   rename(datetime_date = datetime) |> 
  #   filter(parameter <= 31)|> 
  #   #        site_id == site) |> 
  #   mutate(site_id = site,
  #          model_id = 'test_runs3') |> 
  #   filter(as.Date(reference_datetime) > ymd("2022-11-07") ) |>  #remove odd date that has dates one month behind reference datetime
  #   select(reference_datetime, datetime_date, site_id, depth, family, parameter, variable, prediction, model_id)
  # 
  # #remove when done testing 
  # write.csv(df_flare_old_forbind, "R/fDCMdepth_mp/df_flare_old_forbind.csv", row.names = FALSE)
  # df_flare_old_forbind <- read_csv("R/fDCMdepth_mp/df_flare_old_forbind.csv")
  # 
  
  #this is the best forecast (v3) FCR is better. start training with FCR.
  #GLM basic model physics only model (bath, water temp, and depth)
  #GLM-aed: package to attach on top. aquatic ecosystem dynamics. full suite. including all other chem data
  #will have to pull in bathymetry data to calculate schmidt

  #water_temp_4cast_new_url <- "s3://anonymous@bio230121-bucket01/vera4cast/forecasts/parquet/project_id=vera4cast/duration=P1D/variable=Temp_C_mean?endpoint_override=renc.osn.xsede.org"
  fcre_reforecast <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/forecasts/parquet/site_id=fcre/model_id=glm_aed_flare_v3/"),
                                      endpoint_override = 'amnh1.osn.mghpcc.org',
                                      anonymous = TRUE)

  ####variable options from flare####
  flare_variables <- arrow::open_dataset(fcre_reforecast) |>
    filter(reference_datetime > ymd("2026-01-01"))|>
  distinct(variable)|>
    collect()
  #variables are
  #Temp_C_mean (no schmidt stability or thermocline depth- would have to calculate using bathymetry)
  #secchi
  
  #new_flare_forecasts <- arrow::open_dataset(fcre_reforecast)
  #this is where I can get the output for FLARE
  #make sure to pull in for the timeframe I actually want
  
  #future water temp####
  latest_ref <- arrow::open_dataset(fcre_reforecast) |>
    filter(variable == "Temp_C_mean") |>
    summarise(max_ref = max(reference_datetime)) |>
    collect() |>
    pull(max_ref)
  
  tomorrow <- as.POSIXct(Sys.Date() + 1, tz = "UTC")
  
  df_flare_new <- arrow::open_dataset(fcre_reforecast) |>
    filter(
      variable == "Temp_C_mean",
      parameter <= 31,
      reference_datetime == latest_ref,
      datetime >= tomorrow
    ) |>
    collect()
  
  bath <- read_csv("https://pasta.lternet.edu/package/data/eml/edi/1254/1/f7fa2a06e1229ee75ea39eb586577184")
  
  bathFCR <- bath|>
    filter(Reservoir == "FCR")
  
  write.csv(bathFCR, "R/FDCMdepth_mp/bathFCR.csv", row.names = FALSE)
  bathFCR <- read_csv("R/FDCMdepth_mp/bathFCR.csv")
  
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

  # future Schmidt stability from FLARE water temperature profiles####
  # uses bathymetry and temp-at-depth to compute daily Schmidt stability

  # interpolate bathymetry to 0.1m resolution
  bth_depths_interp <- seq(min(bathFCR$Depth_m), max(bathFCR$Depth_m), by = 0.1)
  bth_areas_interp  <- approx(x = bathFCR$Depth_m, y = bathFCR$SA_m2,
                               xout = bth_depths_interp, rule = 2)$y

  bth_depths <- bth_depths_interp
  bth_areas  <- bth_areas_interp

  schmidt_df <- df_flare_new_forbind |>
    group_by(date = as.Date(datetime_date), parameter) |>
    arrange(depth) |>
    summarise(
      schmidt = rLakeAnalyzer::schmidt.stability(
        wtr    = prediction,
        depths = depth,
        bthA   = bth_areas,
        bthD   = bth_depths
      ),
      .groups = "drop"
    ) |>
    # average across ensemble members to get one value per day
    group_by(date) |>
    summarise(schmidt_stability = mean(schmidt, na.rm = TRUE), .groups = "drop") |>
    rename(datetime = date)

  ####future secchi####
  #extc instead 
  latest_ref <- arrow::open_dataset(fcre_reforecast) |>
    filter(variable == "secchi") |>
    summarise(max_ref = max(reference_datetime)) |>
    collect() |>
    pull(max_ref)
  
  tomorrow <- as.POSIXct(Sys.Date() + 1, tz = "UTC")
  
  
  #could also just filter on horizon
  secchi_flare_new <- arrow::open_dataset(fcre_reforecast) |>
    filter(
      variable == "secchi",
      parameter <= 31,
      reference_datetime == latest_ref,
      datetime >= tomorrow
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
  
  
  
  # Get covariate targets####
  #eventually thermocline depth
  #Schmidt stability 
  #secchi
  #average air temperature (from NOAA)
  #average windspeed (from NOAA)
 
  #this function tells me data availability
  source("R/fDCMdepth_mp/data_availability_function.R")
  
  
  #what do we have 
  check <- readr::read_csv(targets_url, show_col_types = F) |>
    filter(site_id %in% site,
           datetime <= forecast_date)|>
    distinct(variable)
  
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
  secchi <- readr::read_csv(targets_url, show_col_types = F) |>
    filter(site_id %in% site,
           datetime <= forecast_date, 
           variable == "Secchi_m_sample")
  secchi_pivot <- secchi|>
    pivot_wider(names_from = variable, values_from = observation)|>
    mutate(Date = as.Date(datetime))
  data_availability(secchi_pivot, "Secchi_m_sample")
  
  ####historic schmidt####
  Schmidt <- readr::read_csv(targets_url, show_col_types = F) |>
    filter(site_id %in% site,
    datetime <= forecast_date, 
    variable == "SchmidtStability_Jm2_mean")
  Schmidt_pivot <- Schmidt|>
    pivot_wider(names_from = variable, values_from = observation)|>
    mutate(Date = as.Date(datetime))
  data_availability(Schmidt_pivot, "SchmidtStability_Jm2_mean")
  
  #not going to include thermocline_depth for now
  #because I am not sure how to address whether or not a thermocline is actually present

  # Assemble historic covariates for RF training####
  historic_covariates <- Schmidt_pivot |>
    select(datetime, SchmidtStability_Jm2_mean) |>
    full_join(
      secchi_pivot |> select(datetime, Secchi_m_sample),
      by = "datetime"
    ) |>
    mutate(datetime = as.Date(datetime))

  covariate_vars <- c("SchmidtStability_Jm2_mean", "Secchi_m_sample")

  # Future FLARE covariates per ensemble member####
  # Schmidt stability per ensemble member from FLARE water temp profiles
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

  # Secchi per ensemble member from FLARE
  future_secchi_ens <- secchi_flare_new_forbind |>
    group_by(datetime_date, parameter) |>
    summarise(Secchi_m_sample = mean(prediction, na.rm = TRUE), .groups = "drop") |>
    mutate(datetime = as.Date(datetime_date)) |>
    select(datetime, parameter, Secchi_m_sample)

  # Combined future covariates
  future_covariates <- future_schmidt_ens |>
    full_join(future_secchi_ens, by = c("datetime", "parameter"))

  #forecasted weather data####
  
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
  
  historic_weather_raw <- arrow::open_dataset(historic_noaa_s3) |>
    filter(datetime < forecast_date,
           variable %in% met_vars) |>
    collect() |>
    group_by(datetime, variable) |>
    summarise(prediction = mean(prediction, na.rm = T), .groups = "drop")
  
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
    mutate(datetime = as.Date(datetime))
  
  
  # Prepare training data####
  message('Preparing training data')
  

  # Fit random forest model for DCM depth####
  message('Fitting random forest model')
  
  # define all possible predictors
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
    left_join(historic_covariates, by = "datetime") |>
    left_join(historic_weather, by = "datetime") |>
    mutate(doy_sin = sin(2 * pi * lubridate::yday(datetime) / 365),
           doy_cos = cos(2 * pi * lubridate::yday(datetime) / 365))
  
  # only keep predictors that actually exist in the data for this site
  predictor_vars <- all_predictor_vars[all_predictor_vars %in% names(fit_df)]
  message(paste0('Using predictors: ', paste(predictor_vars, collapse = ", ")))
  
  # remove NAs for ranger (cannot handle missing values)
  fit_df_noNA <- fit_df |>
    select(all_of(c(var, predictor_vars))) |>
    na.omit()
  
  # fit ranger with quantile regression enabled for uncertainty estimation
  rf_formula <- reformulate(predictor_vars, response = var)
  
  dcm_model <- ranger(rf_formula,
                      data = fit_df_noNA,
                      num.trees = 500,
                      quantreg = TRUE,
                      keep.inbag = TRUE)
  
  # get process uncertainty from OOB residuals
  oob_preds <- dcm_model$predictions
  oob_residuals <- oob_preds - fit_df_noNA[[var]]
  sigma <- sd(oob_residuals, na.rm = TRUE)
  
  message(paste0('Random forest OOB R-squared: ', round(dcm_model$r.squared, 3)))
  message(paste0('Process uncertainty (sigma): ', round(sigma, 3)))
  
  # Set up forecast data frame
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

    # predict using quantile regression forest
    # each ensemble member gets a different quantile of the conditional distribution
    rf_pred <- predict(dcm_model,
                       data = new_data,
                       type = "quantiles",
                       quantiles = quantile_levels)

    # extract the diagonal: member j gets quantile j from its own input row
    dcm_pred$value <- sapply(1:n_members, function(j) rf_pred$predictions[j, j]) +
      rnorm(n = n_members, mean = 0, sd = sigma) # add process uncertainty

    # insert values back into the forecast dataframe
    forecast_full_unc <- forecast_full_unc |>
      rows_update(dcm_pred, by = c("date", "ensemble_member", "forecast_variable", "uc_type"))

  } # end for loop
  
  # clean up file to match vera format
  forecast_df <- forecast_full_unc |>
    rename(datetime = date,
           variable = forecast_variable,
           prediction = value,
           parameter = ensemble_member) |>
    mutate(family = 'ensemble',
           duration = "P1D",
           depth_m = forecast_depths,
           project_id = project_id,
           model_id = model_id,
           site_id = site) |>
    select(datetime, reference_datetime, model_id, site_id,
           parameter, family, prediction, variable, depth_m,
           duration, project_id)
  
  return(forecast_df)
  
} ##### end function

