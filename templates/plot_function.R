generate_forecast_plot <- function(score_df, y_limits){

  
  p <- score_df |> 
    ggplot(aes(x=datetime))+
    geom_ribbon(aes(ymin = quantile10, ymax = quantile90), colour = 'lightblue', fill = 'lightblue') +
    geom_line(aes( y=mean)) +
    geom_point(aes(y = observation)) +
    ggplot2::ylim(y_limits) +
    labs(y = 'Prediction') +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0.2)) +
    theme(text = element_text(size = 10),
          panel.grid.minor = element_blank())
  
  return(p)
  
}
