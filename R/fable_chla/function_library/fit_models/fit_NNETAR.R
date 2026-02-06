#Fit NNETAR model for chl-a
#Author: Mary Lofton
#Date: 10OCT23

#Purpose: fit NNETAR model for chla from 2018-2021

library(fable)
library(feasts)
library(urca)

#'Function to fit NNETAR model for chla
#'@param data data frame with columns Date (yyyy-mm-dd) and
#'median daily EXO_chla_ugL_1 with chl-a measurements in ug/L

fit_NNETAR <- function(data){
  
  #assign target and predictors
  df <- data %>%
    select(-depth_m, -variable) %>%
    as_tsibble(key = site_id, index = datetime)
  
  #fit ARIMA from fable package
  my.nnar <- df %>%
    model(arima = fable::NNETAR(observation)) 
  fitted_values <- fitted(my.nnar)
  
  NNETAR_plot <- ggplot()+
    xlab("")+
    ylab("Chla (ug/L)")+
    geom_point(data = df, aes(x = datetime, y = observation, group = site_id, color = site_id))+
    geom_line(data = fitted_values, aes(x = datetime, y = .fitted, group = site_id, color = site_id))+
    labs(color = NULL, fill = NULL)+
    theme_classic()
  NNETAR_plot

  #build output df
  df.out <- data.frame(site_id = df$site_id,
                       model_id = "NNETAR",
                       datetime = df$datetime,
                       variable = "Chla_ugL_mean",
                       depth_m = 1.6,
                       observation = df$observation,
                       prediction = fitted_values$.fitted)

  
  #return output + model with best fit + plot
  return(list(out = df.out, NNETAR = my.nnar, plot = NNETAR_plot))
}
