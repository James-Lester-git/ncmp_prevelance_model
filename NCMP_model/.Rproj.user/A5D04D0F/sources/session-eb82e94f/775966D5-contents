

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

# If you want survey year 2023/24 input "202324" in ncmp_years, for school year input "6" or "R", selcet sample size.
#Change me
school_year <- "R"
ncmp_years <- c(202324, 202223, 202122)
sample_n <- 10000

# Pull Year 6/R data from x years and save as separate objects and create combined 
get_multiple_pupil_data(con, ncmp_years , school_year, sample_n)
combine_pupil_data(ncmp_years, school_year)

### More spring cleaning and variable selection so it fits into the model 
### For basic replication let's focus on sex, age (in years), height (in meters) & weight. 
if (exists("pupil_data_combined_6")) {
  ncmp_6 <- pupil_data_combined_6 %>%
    select(ageinmonths, gendercode, height, weight, bmizscore)
}

if (exists("pupil_data_combined_R")) {
  ncmp_r <- pupil_data_combined_R %>%
    select(ageinmonths, gendercode, height, weight, bmizscore)
}

#### Change variables to format needed #### 
prepare_ncmp_data_for_model <- function(df) {
  df %>%
    transmute(
      age = ageinmonths / 12, # Convert months to years
      sex = case_when(
        gendercode == "M" ~ 1,
        gendercode == "F" ~ 2,
        TRUE ~ NA_real_
      ),
      height = height/100,
      weight = weight,
      bmizscore = bmizscore,
      wt = 1 
    ) %>%
    drop_na(age, sex, height, weight)
}

if (exists("ncmp_6")) {
  ncmp_6 <- prepare_ncmp_data_for_model(ncmp_6) 
}

if (exists("ncmp_r")) {
  ncmp_r <- prepare_ncmp_data_for_model(ncmp_r) 
}


# Function to classify BMI using z-scores (child growth standards)
obese_and_bmi <- function(data){
  data %>%
    mutate(bmi = weight / (height^2)) %>%
    mutate(z = LMS2z(age, bmi, sex, "bmi", "uk90")) %>%
    mutate(percentile = pnorm(z)) %>%
    mutate(bmi_cat = case_when(
      percentile <= 0.02 ~ 1,
      percentile > 0.02 & percentile < 0.85 ~ 2,
      percentile >= 0.85 & percentile < 0.95 ~ 3,
      percentile >= 0.95 & percentile < 0.996 ~ 4,
      percentile >= 0.996 ~ 5
    )) %>%
    select(-z, -percentile)
}

# Apply BMI categorisation to your NCMP data

if (exists("ncmp_6")) {
  ncmp_6 <- obese_and_bmi(ncmp_6) 
}

if (exists("ncmp_r")) {
  ncmp_r <- obese_and_bmi(ncmp_r) 
}