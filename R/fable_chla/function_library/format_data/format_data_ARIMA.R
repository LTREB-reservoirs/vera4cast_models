#Format data for ARIMA model for chl-a
#Author: Mary Lofton
#Date: 10OCT23

#Purpose: format data for ARIMA model for chla from 2018-present

#'Function to fit day of year model for chla
#'@param targets filepath to exo targets for VERA
#'@param end_date yyyy-mm-dd today's date

#load packages
library(tidyverse)
library(lubridate)
library(zoo)

format_data_ARIMA <- function(targets, end_date){
  
  #read in targets 
  dat <- read_csv(targets) %>%
    filter(variable == "Chla_ugL_mean") %>%
    arrange(site_id, datetime)
  
  sites <- unique(dat$site_id)
  
  start_dates <- dat %>%
    group_by(site_id) %>%
    filter(!is.na(observation)) %>%
    slice(1) %>%
    pull(datetime)

  #get list of dates
  end_date = as.Date(end_date)
  daily_dates_df = tibble(datetime = Date(length = 0L), 
                          site_id = character(length = 0L))
  for(i in 1:length(start_dates)){
  temp_dates <- seq.Date(from = as.Date(start_dates[i]), to = as.Date(end_date), by = "day")
  temp_sites <- rep(sites[i], times = length(temp_dates))
  temp_df <- tibble(datetime = temp_dates,
                    site_id = temp_sites)
  daily_dates_df <- bind_rows(daily_dates_df, temp_df)
  }
  
  #join to dates and interpolate
  dat1 <- left_join(daily_dates_df, dat, by = c("datetime","site_id")) %>%
    group_by(site_id) %>%
    mutate(observation = interpolate(observation))
    
return(dat1)
}
