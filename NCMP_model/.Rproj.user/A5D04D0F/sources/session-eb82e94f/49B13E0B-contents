#### NCMP Prevelance Model      #### 
#### Adapting the OG Prevelance ####
#### model to the NCMP Data set ####


setwd("Z:/IA_NCMP/")
source("NCMP_prevelance_model/NCMP_model/0.functions.R") 
source("NCMP_prevelance_model/NCMP_model/1.loading_and_cleaning.R") 


#################################
#### SECTION 1 - PARAMETERS #####
#################################
#Look in "1.loading_and_cleaning.R" to decide year group, survey years and sample size.
school_group <- "6"  # Either "6" or "R"
bmi_targetting <- 0
num_targetting <- 100
proportion_in_target <- num_targetting / 100


data <- get_school_group_data(school_group) # see 0.functions.R

# Get indices and sample size of the target group
target <- get_target_sample(data, bmi_targetting = bmi_targetting, proportion = proportion_in_target)

numbers_to_match <- target$indices
sample_size <- target$size
eligible_data <- target$eligible_data

###############################################
###############################################
#### SECTION 3 - HENRY EQUATION PARAMETERS ####
### HENRY 2005 - table 12 or SCAN table 19 ####
###############################################

# Activity multiplier
#=============================================================

# FUNCTION TO ADD HENRY PARAMETERS (applies to both "6" and "R")
activity <- function(age) {
  case_when(
    age >= 4 & age <6 ~ 1.57,
    age >= 10 & age < 12 ~ 1.73,
    TRUE ~ 1.63
  )
}

weight_multiplier <- function(age, sex) {
  case_when(
    sex == 1 & age >= 4 & age < 6 ~ 23.3,
    sex == 1 & age >= 10 & age < 12 ~ 18.4,
    sex == 2 & age >= 4 & age < 6 ~ 20.1,
    sex == 2 & age >= 10 & age < 12 ~ 11.1,
    TRUE ~ NA_real_
  )
}

constant <- function(age, sex) {
  case_when(
    sex == 1 & age >= 4 & age < 6 ~ 514,
    sex == 1 & age >= 10 & age < 12 ~ 581,
    sex == 2 & age >= 4 & age < 6 ~ 507,
    sex == 2 & age >= 10 & age < 12 ~ 761,
    TRUE ~ NA_real_
  )
}

mutate_add_henry_parameters <- function(data) {
  data %>%
    mutate(
      activity = activity(age),
      weight_multiplier = weight_multiplier(age, sex),
      constant = constant(age, sex)
    )
}

############################################################################
############################################################################
#### SECTION 4 - FUNCTIONS TO CALCULATE NEW OBESITY RATE AND BMI CHANGE ####
############################################################################
############################################################################
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
obesity_rate <- function(data, weighting = TRUE) {
  if (!weighting) {
    data <- mutate(data, wt = 1)
  }
  
  data %>%
    mutate(new_bmi = new_weight / (height^2)) %>%
    mutate(
      bmi_change = ifelse(bmi - new_bmi < 0.0001, NA, bmi - new_bmi),
      weight_change = ifelse(weight - new_weight < 0.0001, NA, weight - new_weight),
      z = LMS2z(age, new_bmi, sex, "bmi", "uk90"),
      percentile = pnorm(z),
      obese_and_very = percentile >= 0.95,
      under = percentile <= 0.02,
      healthy = percentile > 0.02 & percentile < 0.85,
      over = percentile >= 0.85 & percentile < 0.95,
      obese = percentile >= 0.95 & percentile < 0.996,
      very = percentile >= 0.996
    ) %>%
    summarise(
      obesity_rate = weighted.mean(obese_and_very, wt, na.rm = TRUE),
      underweight = weighted.mean(under, wt, na.rm = TRUE),
      healthy_weight = weighted.mean(healthy, wt, na.rm = TRUE),
      overweight = weighted.mean(over, wt, na.rm = TRUE),
      obese = weighted.mean(obese, wt, na.rm = TRUE),
      very_obese = weighted.mean(very, wt, na.rm = TRUE),
      av_bmi_decrease_of_targeted = weighted.mean(bmi_change, wt, na.rm = TRUE),
      av_weight_decrease_of_targeted = weighted.mean(weight_change, wt, na.rm = TRUE)
    )
}




# FUNRTION TO ADD A NUMERIC COUNTER TO INDIVIDUALS IN THE TARGET BMI CATs
#---------------------------------------------------------------------------
add_num <- function(data) {
  data <- data %>% mutate(row_id = row_number())  # unique key
  
  target_rows <- data %>%
    filter(bmi_cat > bmi_targetting) %>%
    mutate(num = row_number()) %>%
    select(row_id, num)
  
  left_join(data, target_rows, by = "row_id")
}


#######################################################
#######################################################
#### SECTION 5 - FUNCTION TO APPLY HENRY EQUATIONS ####
#######################################################
#######################################################

apply_kcal_reduction <- function(data, kcal_reduction) {
  data %>%
    mutate_add_henry_parameters() %>%
    mutate(kcal = activity * (weight_multiplier * weight + constant)) %>%
    mutate(kcal = if_else(bmi_cat > bmi_targetting & num %in% numbers_to_match,
                          kcal - kcal_reduction, kcal)) %>%
    mutate(new_weight = kcal / (activity * weight_multiplier) - constant / weight_multiplier) %>%
    mutate(new_weight = if_else(new_weight < 0, 1, new_weight)) %>%
    select(-kcal)
}

###########################################################################
###########################################################################
#### SECTIO 7 - FUNCTION TO APPLY CHANGE AND CALCULATE NEW OBESITY RATE ###
###########################################################################
###########################################################################

new_obesity_rate <- function(kcal, weighted=T) {
  data %>%
    obese_and_bmi %>% # CALCULATE THE OBESITY CATS AND CURRENT BMIs
    add_num %>% # NUMBER THE TARGETTED BMI CATS
    apply_kcal_reduction(kcal) %>% # apply henry equations with kcal reduction to calculate new weight
    obesity_rate(weighted) %>% # calculate new obesity rate
    mutate(kcal = kcal)
}

#################################################################
#################################################################
#### SECTION 6 - USE FUNCTIONS TO CALCULATE AND WRITE OUTPUT ####
#################################################################
#################################################################

plan(multisession(workers = 4))

inputs <- c(seq(0,500, by = 1)) # kcal reductions to apply

inputs <- c(inputs, -inputs) %>% # add calorie decreases as well as increases
  unique()

future_map_dfr(inputs, new_obesity_rate, .progress = TRUE) %>%
  write_csv(sprintf("NCMP_prevelance_model/NCMP_model/outputs/year%s_targeted_%dpct_bmi_gt_%d_sample_%d.csv",
                    school_group, num_targetting, bmi_targetting, sample_size))


if(sample_size<=100){
  dlg_message('CAUTION SMALL SAMPLE SIZE!!!','yesno')$res 
}