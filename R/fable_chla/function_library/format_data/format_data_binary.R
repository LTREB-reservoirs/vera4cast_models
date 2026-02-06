#Format data for binary phyto variables
#Author: Mary Lofton
#Date: 15NOV23

#Purpose: format data for ARIMA model for chla from 2018-present

#'Function to fit day of year model for chla
#'@param targets filepath to exo targets for VERA
#'@param end_date yyyy-mm-dd today's date

#load packages
library(tidyverse)
library(lubridate)
library(zoo)

format_data_binary <- function(targets, end_date){
  
  #read in targets 
  dat <- read_csv(targets) %>%
    filter(variable %in% c("Bloom_binary_mean","DeepChlorophyllMaximum_binary_sample","Temp_C_mean","Secchi_m_sample") & duration == "P1D") %>%
    arrange(site_id, variable, datetime) %>%
    mutate(datetime = date(datetime)) %>%
    group_by(site_id) %>%
    pivot_wider(id_cols = c(site_id, datetime), names_from = variable, values_from = observation, values_fn = mean) %>%
    filter_at(vars(Bloom_binary_mean:Temp_C_mean),any_vars(!is.na(.))) %>%
    mutate(doy = yday(datetime)) %>%
    ungroup()
    
return(dat)
}
