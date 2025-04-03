#### Script to store functions ####

#####Read in and basic cleaning ####
get_clean_pupil_data <- function(con, ncmp_year, school_years = c("6"), sample_n = 10000) {
  school_years_sql <- paste0("'", school_years, "'", collapse = ", ")
  
  query <- sprintf("
    SELECT TOP (%d)
        [Period],
        [NCMPyear],
        [Pupil_ID],
        [GenderCode],
        [AgeInMonths],
        [MonthOfMeasurement],
        [SchoolYear],
        [NhsEthnicityCode],
        [Ethnicity_desc],
        [Height],
        [HeightZScore],
        [HeightPScore],
        [Weight],
        [WeightZScore],
        [WeightPScore],
        [BMI],
        [BmiZScore],
        [BmiPScore],
        [BmiClinicalCategory],
        [BmiPopulationCategory]
    FROM [NCMP].[dbo].[vPupil_data]
    WHERE [NCMPyear] = %d
    AND [Exclude_flag] = 0
      AND [SchoolYear] IN (%s)
      AND [BMI] > 0 AND [BMI] <= 50
      AND [Height] BETWEEN 60 AND 200
      AND [Weight] BETWEEN 10 AND 120
      AND [AgeInMonths] BETWEEN 48 AND 132
      AND ABS([HeightZScore]) <= 5
      AND ABS([WeightZScore]) <= 5
      AND ABS([BmiZScore]) <= 5
      AND [GenderCode] IN ('M', 'F')
      AND [NhsEthnicityCode] IS NOT NULL AND [NhsEthnicityCode] <> ''
      AND [Pupil_ID] IS NOT NULL
    ORDER BY NEWID()
  ", sample_n, ncmp_year, school_years_sql)
  
  dbGetQuery(con, query)
}

###### Multiple Years ####
get_multiple_pupil_data <- function(con, ncmp_years, school_year = "6", sample_n = 10000) {
  for (year in ncmp_years) {
    data <- get_clean_pupil_data(
      con,
      ncmp_year = year,
      school_years = c(school_year),
      sample_n = sample_n
    )
    
    colnames(data) <- stringr::str_to_lower(colnames(data))
    
    obj_name <- paste0("pupil_data_", year, "_", school_year)
    assign(obj_name, data, envir = .GlobalEnv)
  }
}


combine_pupil_data <- function(years, school_year) {
  obj_names <- paste0("pupil_data_", years, "_", school_year)
  
  # Keep only the objects that exist in the environment
  existing_dfs <- obj_names[obj_names %in% ls(envir = .GlobalEnv)]
  
  # Combine and select relevant columns
  combined <- existing_dfs %>%
    map(~ get(.x, envir = .GlobalEnv)) %>%
    bind_rows()
  
  # Create the dynamic output name like "pupil_data_combined_6"
  out_name <- paste0("pupil_data_combined_", school_year)
  
  # Assign to global environment
  assign(out_name, combined, envir = .GlobalEnv)
  
  # Optionally return it
  return(invisible(combined))
}



get_target_sample <- function(data, bmi_targetting = 0, proportion = 1.0) {
  if (!"bmi_cat" %in% names(data)) {
    data <- obese_and_bmi(data)
  }
  
  # Filter for eligible population
  eligible <- data %>%
    filter(
      case_when(
        bmi_targetting == 0 ~ TRUE,                       # Everyone
        bmi_targetting == 1 ~ bmi_cat >= 3,               # Overweight and above
        bmi_targetting == 2 ~ bmi_cat >= 4,               # Obese and above
        bmi_targetting == 3 ~ bmi_cat == 5,               # Very obese only
        bmi_targetting == 4 ~ bmizscore >= 2.67,          # >99.6th percentile
        TRUE ~ FALSE
      )
    )
  
  # Sample the target group
  n <- nrow(eligible)
  sample_indices <- sample(n, n * proportion)
  
  list(
    indices = sample_indices,
    size = length(sample_indices),
    eligible_data = eligible
  )
}


get_school_group_data <- function(school_group) {
  if (school_group == "6") {
    ncmp_6 %>% filter(age >= 10 & age <= 11)
  } else if (school_group == "R") {
    ncmp_r %>% filter(age >= 4 & age <= 6)
  } else {
    stop("school_group must be either '6' or 'R'")
  }
}
