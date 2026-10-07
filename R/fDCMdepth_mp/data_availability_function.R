
library(ggplot2)
library(dplyr)
library(patchwork)  # For combining plots

# Define the function to handle multiple variables
#takes dataframe and list of variables
data_availability <- function(dataframe, variables) {
  # Generate plots for each variable
  plot_list <- lapply(variables, function(var) {
    var_sym <- sym(var)  # Convert string to symbol
    
    # Filter and process data
    plot_dat <- dataframe %>%
      filter(!is.na(!!var_sym)) %>%
      mutate(Year = year(Date), DayOfYear = yday(Date)) %>%
      select(Date, Year, DayOfYear, !!var_sym)
    
    # Find the maximum value for each year
    max_values_per_year <- plot_dat %>%
      group_by(Year) %>%
      slice(which.max(!!var_sym)) %>%
      ungroup()
    
    # Create the plot
    p <- ggplot(plot_dat, aes(x = DayOfYear, y = as.factor(Year), group = Year)) +
      geom_line() +
      geom_point(shape = 18) +
      geom_point(data = max_values_per_year, aes(x = DayOfYear, y = as.factor(Year)), 
                 color = "red", size = 2) +
      geom_text(data = max_values_per_year, 
                aes(x = DayOfYear, y = as.factor(Year), 
                    label = paste0("Max: ", round(!!var_sym, 2))), 
                vjust = 1.5, hjust = 0.5, color = "black", size = 3) +
      theme_bw() +
      labs(x = "Day of Year", y = "Year", title = paste("Data Availability:", var)) +
      scale_x_continuous(breaks = seq(1, 365, by = 30), limits = c(1, 365)) +
      theme(panel.grid.minor = element_blank()) +
      geom_vline(xintercept = 133, linetype = "dashed", color = "red") +
      geom_vline(xintercept = 286, linetype = "dashed", color = "red")
    
    return(p)
  })
  
  # Combine all plots into one panel figure
  final_plot <- wrap_plots(plot_list, ncol = 3)  # Adjust number of columns as needed
  return(final_plot)
}