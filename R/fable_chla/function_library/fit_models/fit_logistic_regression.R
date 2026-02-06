#Fit logistic regression model for binary phyto vars
#Author: Mary Lofton
#Date: 28FEB23

#Purpose: fit logistic regression models for bloom and DCM binary variables

#'Function to fit day of year model for chla
#'@param data data frame with columns Date (yyyy-mm-dd) and
#'median daily EXO_chla_ugL_1 with chl-a measurements in ug/L

fit_logistic_regression <- function(data){
  
  #get combinations of sites and variables
  sites <- unique(data$site_id)
  target_vars <- colnames(data)[grepl("binary",colnames(data))]
  
  for(i in 1:length(sites)){
    for(j in 1:length(target_vars)){
      #assign target and predictors
      df <- data %>%
        filter(site_id == sites[i]) %>%
        select(Temp_C_mean, Secchi_m_sample, doy, target_vars[j]) %>%
        filter(complete.cases(.))
      
      #fit logistic regression
      mod <- as.formula(sprintf("%s ~ .", target_vars[j]))
      
      model <- glm(formula = mod,family=binomial(link='logit'),data=df)
      summary(model)
    }
  }
  
  ETS_plot <- ggplot()+
    xlab("")+
    ylab("Chla (ug/L)")+
    geom_point(data = df, aes(x = datetime, y = observation, group = site_id, color = site_id))+
    geom_line(data = fitted_values, aes(x = datetime, y = .fitted, group = site_id, color = site_id))+
    labs(color = NULL, fill = NULL)+
    theme_classic()
  ETS_plot

  #build output df
  df.out <- data.frame(site_id = df$site_id,
                       model_id = "ETS",
                       datetime = df$datetime,
                       variable = "Chla_ugL_mean",
                       depth_m = 1.6,
                       observation = df$observation,
                       prediction = fitted_values$.fitted)

  
  #return output + model with best fit + plot
  return(list(out = df.out, ETS = my.ets, plot = ETS_plot))
}
