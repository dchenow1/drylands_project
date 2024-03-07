# R code stolen from SWW on 2/29/24 and adapted by DAC & chatGPT
#library(here)
#setwd(here())
library(readr)
library(rgdal)
library(ggplot2)
library(tidyverse)
library(lubridate)
library(devtools)
library(remotes)
library(dplyr)
library(zoo)
library(Surrogate)
library(daymetr)
library(stringr)
library(rSOILWAT2)

setwd("your directory")

markov_destination_directory <- paste(getwd(),"/test_folder",sep = "")
main_directory <- paste(getwd(),"/site_weather_data",sep = "")

# Define the main directory path
# Initialize an empty list to store all data frames
all_data <- list()
# List all directories within the main directory
sub_directories <- list.dirs(main_directory, recursive = FALSE)
# Loop through each sub-directory
for (sub_dir in sub_directories) {
  # List all CSV files in the sub-directory
  csv_files <- list.files(sub_dir, pattern = "\\.csv$", full.names = TRUE)
  # Loop through each CSV file in the sub-directory and read it
  for (csv_file in csv_files) {
    # Read the CSV file
    data <- read.csv(csv_file)
    # Extract the filename without extension
    filename <- tools::file_path_sans_ext(basename(csv_file))
    # Remove unnecessary columns
    data <- data[, 4:8]
    # Change names of variables to match SOILWAT2
    names(data) <- c("PPT_cm", "Tmax_C", "Tmin_C", "Year", "DOY")
    # Convert PPT_cm from kg/m^2/s to mm
    data$PPT_cm <- data$PPT_cm*8640
    # Convert temperature from Kelvin to Celsius
    data[, c("Tmax_C", "Tmin_C")] <- data[, c("Tmax_C", "Tmin_C")] - 273.15
    # Reorder columns
    data <- data[, c("Year", "DOY", "Tmax_C", "Tmin_C", "PPT_cm")]
    # Store the data frame in the all_data list with the filename as the list name
    all_data[[filename]] <- data
  }
}

setwd(markov_destination_directory)

for (wdb in seq_along(all_data)) {
  #print(seq_along(all_data)[wdb])
  
  tryCatch({
    name <- paste(names(all_data[wdb]), "_wdb")
    print(name)
    directory_parts <- strsplit(name, "_")[[1]]
    first_part <- directory_parts[1]
    last_part <- paste(directory_parts[-1], collapse = "_")
    directory_path <- getwd()  # Start with current working directory
    
    # Create directories based on the name convention
    if (!dir.exists(file.path(directory_path, first_part))) {
      dir.create(file.path(directory_path, first_part), recursive = TRUE)  # Create directory if it doesn't exist
    }
    
    if (!dir.exists(file.path(directory_path, first_part, last_part))) {
      dir.create(file.path(directory_path, first_part, last_part), recursive = TRUE)  # Create directory if it doesn't exist
    }
    
    # Continue with your existing code logic...
    
    if (!inherits(i, "try-error")) {
      assign(name, rSOILWAT2::dbW_dataframe_to_weatherData(weatherDF = all_data[[wdb]]))
      wdata = rSOILWAT2::dbW_dataframe_to_weatherData(weatherDF = all_data[[wdb]])
      # Convert `DayMet`'s `noleap` calendar to proleptic Gregorian calendar
      wdata <- rSOILWAT2::dbW_convert_to_GregorianYears(weatherData = wdata)
      assign(name, rSOILWAT2::dbW_generateWeather(
        weatherData = rSOILWAT2::dbW_dataframe_to_weatherData(weatherDF = wdata),
        seed = 123
      ))
      wdata = rSOILWAT2::dbW_generateWeather(
        weatherData = rSOILWAT2::dbW_dataframe_to_weatherData(weatherDF = wdata),
        seed = 123
      )
      # Check that weather data is well-formed
      stopifnot(rSOILWAT2::dbW_check_weatherData(wdata))
    } 
    
    #Get coefficient data frames from the weather data for 'wdata'
    mfs <- dbW_estimate_WGen_coefs(wdata, imputation_type = "mean", imputation_span = 5)
    #Comment out the column names for STEPWAT2 to read
    names(mfs[[1]])[1] <- paste0("#", names(mfs[[1]])[1])
    names(mfs[[2]])[1] <- paste0("#", names(mfs[[2]])[1])
    
    #Write tables for each markov file
    write.table(format(mfs[[1]], digits = 6), paste(directory_path, first_part, last_part, "mkv_covar.in", sep = "/"), quote = FALSE, row.names = FALSE)
    write.table(format(mfs[[2]], digits = 6), paste(directory_path, first_part, last_part, "mkv_prob.in", sep = "/"), quote = FALSE, row.names = FALSE)
  }, error = function(e) {
    # Check if the error is due to temperature inconsistency
    if (grepl("Daily input value for minimum temperature is greater than daily input value for maximum temperature", conditionMessage(e))) {
      message("Skipping iteration due to temperature inconsistency.")
      return()  # Skip this iteration
    } else {
      # Print other error messages
      message("An error occurred: ", conditionMessage(e))
      next  # Continue to the next iteration
    }
  })
}











