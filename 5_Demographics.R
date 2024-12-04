# demographics

# calculate age from DOB
library(lubridate)

dataset <- dataset %>%
  mutate(
    DOB = ymd_hms(date_of_birth),  # Convert to Date-time format
    age = as.numeric(difftime(Sys.Date(), DOB, units = "days")) / 365.25,  # Calculate age in years
    all=TRUE
  )



# Summary function to calculate demographics by logical variables
summary_table <- dataset %>%
  select(age, gender, race, ethnicity, all,
                 PHD, AC1, Clean_PAH1, Clean_PAH2) %>%
  pivot_longer(cols = c(all, PHD, AC1, Clean_PAH1, Clean_PAH2),
               names_to = "logical_variable", values_to = "value") %>%
  group_by(logical_variable, value) %>%
  summarise(
    count = n(),
    mean_age = mean(age, na.rm = TRUE),
    gender_male = mean(gender == "Male") * 100,
    race_white = mean(race == "White") * 100,
    race_black = mean(race == "Black or African American") * 100,
    ethnicity_latino = mean(ethnicity=="Hispanic or Latino") * 100,
    .groups = "drop"
  ) %>%
  arrange(logical_variable, value) %>%
  filter(value==TRUE) %>%
  select(!value)
  

# View the summary table
print(summary_table)