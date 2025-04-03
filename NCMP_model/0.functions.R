#### Script to store functions ####

#####Read in and basic cleaning ####
get_clean_pupil_data <- function(con, ncmp_year, school_years = c("6"), sample_n = 10000, deprivation = NULL) {
  school_years_sql <- paste0("'", school_years, "'", collapse = ", ")
  
  # Handle deprivation filtering (single value or vector)
  deprivation_filter <- if (!is.null(deprivation)) {
    values <- paste(as.integer(deprivation), collapse = ", ")
    sprintf("AND [PupilIndexOfMultipleDeprivationD] IN (%s)", values)
  } else {
    ""
  }
  
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
        [BmiPopulationCategory], 
        [PupilIndexOfMultipleDeprivationD]
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
      %s
    ORDER BY NEWID()
  ", sample_n, ncmp_year, school_years_sql, deprivation_filter)
  
  dbGetQuery(con, query)
}


run_ncmp_pipeline <- function(con, 
                              ncmp_years, 
                              school_year = "6", 
                              sample_n = 10000, 
                              deprivation = NULL) {
  
  # Step 1: Load data for multiple years
  get_multiple_pupil_data(con, ncmp_years, school_year, sample_n, deprivation)
  

  combine_pupil_data(ncmp_years, school_year, deprivation)
  
  create_ncmp_objects_from_combined(school_years = school_year)
  
  processed_names <- process_ncmp_objects(school_years = school_year)
  
  # Step 5: Return the cleaned object if it exists
  if (length(processed_names) == 1 && exists(processed_names, envir = .GlobalEnv)) {
    message("✅ Returning object: ", processed_names)
    return(get(processed_names, envir = .GlobalEnv))
  } else if (length(processed_names) > 1) {
    warning("⚠️ Multiple processed objects found. Returning first.")
    return(get(processed_names[[1]], envir = .GlobalEnv))
  } else {
    warning("❌ Could not find processed object to return.")
    return(NULL)
  }
}

###### Multiple Years ####
get_multiple_pupil_data <- function(con, ncmp_years, school_year = "6", sample_n = 10000, deprivation = NULL) {
  
  # Create a deprivation label for object naming
  deprivation_label <- if (!is.null(deprivation)) {
    dep_vals <- as.integer(deprivation)
    if (length(dep_vals) == 1) {
      paste0("_dep", dep_vals)
    } else {
      paste0("_dep", min(dep_vals), "_", max(dep_vals))
    }
  } else {
    ""
  }
  
  for (year in ncmp_years) {
    data <- get_clean_pupil_data(
      con,
      ncmp_year = year,
      school_years = c(school_year),
      sample_n = sample_n,
      deprivation = deprivation
    )
    
    colnames(data) <- stringr::str_to_lower(colnames(data))
    
    # Build object name with deprivation info
    obj_name <- paste0("pupil_data_", year, "_", school_year, deprivation_label)
    
    assign(obj_name, data, envir = .GlobalEnv)
  }
}





combine_pupil_data <- function(ncmp_years, school_year = "6", deprivation = NULL) {
  # Create a deprivation label to match get_multiple_pupil_data
  deprivation_label <- if (!is.null(deprivation)) {
    dep_vals <- as.integer(deprivation)
    if (length(dep_vals) == 1) {
      paste0("_dep", dep_vals)
    } else {
      paste0("_dep", min(dep_vals), "_", max(dep_vals))
    }
  } else {
    ""
  }
  
  # Combine objects
  combined_data <- purrr::map_dfr(ncmp_years, function(year) {
    obj_name <- paste0("pupil_data_", year, "_", school_year, deprivation_label)
    if (exists(obj_name, envir = .GlobalEnv)) {
      get(obj_name, envir = .GlobalEnv)
    } else {
      warning(paste("Object", obj_name, "not found. Skipping."))
      NULL
    }
  })
  
  # Assign combined object to global env
  combined_obj_name <- paste0("pupil_data_combined_", school_year, deprivation_label)
  assign(combined_obj_name, combined_data, envir = .GlobalEnv)
}

# Create ncmp_* objects from the combined pupil_data_combined_* ones
create_ncmp_objects_from_combined <- function(school_years = c("6", "R")) {
  for (sy in school_years) {
    combined_obj_names <- ls(pattern = paste0("^pupil_data_combined_", sy, "(_dep\\d+(_\\d+)?)?$"))
    
    for (combined_name in combined_obj_names) {
      # Create corresponding ncmp_* object name
      ncmp_name <- sub("pupil_data_combined_", "ncmp_", combined_name)
      
      message("Creating: ", ncmp_name)
      
      df <- get(combined_name, envir = .GlobalEnv)
      
      # Keep only the core vars
      df <- df %>%
        select(ageinmonths, gendercode, height, weight, bmizscore)
      
      assign(ncmp_name, df, envir = .GlobalEnv)
    }
  }
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

#### Change variables to format needed #### 
# -- PREP FUNCTIONS --

prepare_ncmp_data_for_model <- function(df) {
  df %>%
    transmute(
      age = ageinmonths / 12,
      sex = case_when(
        gendercode == "M" ~ 1,
        gendercode == "F" ~ 2,
        TRUE ~ NA_real_
      ),
      height = height / 100,
      weight = weight,
      bmizscore = bmizscore,
      wt = 1
    ) %>%
    drop_na(age, sex, height, weight)
}

obese_and_bmi <- function(data) {
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

# -- FLEXIBLE OBJECT FINDER + PROCESSOR --

process_ncmp_objects <- function(school_years = c("6", "R")) {
  processed <- c()
  
  for (sy in school_years) {
    obj_names <- ls(pattern = paste0("^ncmp_", sy, "(_dep\\d+(_\\d+)?)?$"))
    
    if (length(obj_names) == 1) {
      message("✅ Processing: ", obj_names)
      
      df <- get(obj_names)
      df <- df %>%
        prepare_ncmp_data_for_model() %>%
        obese_and_bmi()
      
      assign(obj_names, df, envir = .GlobalEnv)
      processed <- c(processed, obj_names)
      
    } else if (length(obj_names) > 1) {
      warning("⚠️ Multiple ncmp objects found for school year ", sy, ": ", paste(obj_names, collapse = ", "))
    } else {
      message("❌ No matching object found for school year ", sy)
    }
  }
  
  return(processed)
}
