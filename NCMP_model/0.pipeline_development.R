run_ncmp_pipeline <- function(con, ncmp_years, school_year = "6", sample_n = 10000, deprivation = NULL) {
  # Load and combine raw data
  raw_data <- purrr::map_dfr(ncmp_years, function(year) {
    get_clean_pupil_data(con, year, school_years = school_year, sample_n = sample_n, deprivation = deprivation)
  })
  
  # Clean and process
  cleaned <- raw_data %>%
    dplyr::rename_all(tolower) %>%
    dplyr::select(ageinmonths, gendercode, height, weight, bmizscore) %>%
    prepare_ncmp_data_for_model() %>%
    obese_and_bmi()
  
  return(cleaned)
}

# --- Core Functions ---

get_clean_pupil_data <- function(con, ncmp_year, school_years = c("6"), sample_n = 10000, deprivation = NULL) {
  school_years_sql <- paste0("'", school_years, "'", collapse = ", ")
  
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

prepare_ncmp_data_for_model <- function(df) {
  df %>%
    dplyr::transmute(
      age = ageinmonths / 12,
      sex = dplyr::case_when(
        gendercode == "M" ~ 1,
        gendercode == "F" ~ 2,
        TRUE ~ NA_real_
      ),
      height = height / 100,
      weight = weight,
      bmizscore = bmizscore,
      wt = 1
    ) %>%
    tidyr::drop_na(age, sex, height, weight)
}

obese_and_bmi <- function(data) {
  data %>%
    dplyr::mutate(
      bmi = weight / (height^2),
      z = LMS2z(age, bmi, sex, "bmi", "uk90"),
      percentile = pnorm(z),
      bmi_cat = dplyr::case_when(
        percentile <= 0.02 ~ 1,
        percentile > 0.02 & percentile < 0.85 ~ 2,
        percentile >= 0.85 & percentile < 0.95 ~ 3,
        percentile >= 0.95 & percentile < 0.996 ~ 4,
        percentile >= 0.996 ~ 5
      )
    ) %>%
    dplyr::select(-z, -percentile)
}
