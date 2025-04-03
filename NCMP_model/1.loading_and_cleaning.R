#Set working directory for Data Lake
setwd("Z:/IA_NCMP/")

##Load packages
if (!require(furrr)) install.packages('furrr')
if (!require(haven)) install.packages("haven")
if (!require(tidyverse)) install.packages("tidyverse")
if (!require(readr)) install.packages("readr")
if (!require(odbc)) install.packages("odbc")
if (!require(sitar)) install.packages('sitar')
if (!require(svDialogs)) install.packages('svDialogs')
library(tidyverse)
library(haven) 
library(readr)
library(DBI)
library(odbc)
library(furrr)
library(sitar)
library('svDialogs')

# Connecting to the SQL Server and Datalake
con <- dbConnect(odbc(), .connection_string = 'driver={SQL Server}; server=SQLClusColLK19\\Lake19; database = trusted_connection=true; timeout=120')

##Function which loads in a random 1000 from respective years, and does basic logical cleaning

source("NCMP_prevelance_model/NCMP_model/0.functions.R") 
source("NCMP_prevelance_model/NCMP_model/0.pipeline_development.R")

# If you want survey year 2023/24 input "202324" in ncmp_years, for school year input "6" or "R", selcet sample size.
#Change me


school_year <- "R"
ncmp_years <- c(202324, 202223, 202122)
sample_n <- 10000
deprivation <- 1

ncmp_data <- run_ncmp_pipeline(con, ncmp_years, school_year, sample_n, deprivation)

run_ncmp_pipeline(con, ncmp_years, school_year, sample_n, deprivation)

# Pull Year 6/R data from x years and save as separate objects and create combined 
get_multiple_pupil_data(con, ncmp_years , school_year, sample_n, deprivation )
combine_pupil_data(ncmp_years, school_year, deprivation)


create_ncmp_objects_from_combined(school_years = school_year)

### More spring cleaning and variable selection so it fits into the model 
### For basic replication let's focus on sex, age (in years), height (in meters) & weight. 
### Select and clean Year 6 data (any deprivation combo)

ncmp_6_obj_name <- ls(pattern = "^pupil_data_combined_6(_dep\\d+(_\\d+)?)?$")
if (length(ncmp_6_obj_name) == 1) {
  ncmp_6 <- get(ncmp_6_obj_name) %>%
    select(ageinmonths, gendercode, height, weight, bmizscore)
} else if (length(ncmp_6_obj_name) > 1) {
  warning("Multiple Year 6 objects found. Please clarify.")
}

### Select and clean Year R data (any deprivation combo)
ncmp_r_obj_name <- ls(pattern = "^pupil_data_combined_R(_dep\\d+(_\\d+)?)?$")
if (length(ncmp_r_obj_name) == 1) {
  ncmp_r <- get(ncmp_r_obj_name) %>%
    select(ageinmonths, gendercode, height, weight, bmizscore)
} else if (length(ncmp_r_obj_name) > 1) {
  warning("Multiple Year R objects found. Please clarify.")
}


process_ncmp_objects(school_years = school_year)

