


# ebben meghagyok minden változót a 2 = KETTŐ körös kísérletekkel

# cornerrel és anélkül is van opció itt


rm(list=ls())

library(arrow)
library(dplyr)
library(stringr)
library(openxlsx)
library(tidytext)

final <- read_parquet("5_all_overtakes_final/final_dataset_flattered.parquet")

#### transzformációk ####

fin <- final %>%
  mutate(
    Rainfall = if_else(Rainfall == TRUE, 1L, 0L),
    FreshTyre_Overtaker = if_else(FreshTyre_Overtaker == TRUE, 1L, 0L),
    FreshTyre_Passed = if_else(FreshTyre_Passed == TRUE, 1L, 0L),
    Team_Overtaker = case_when(
      Team_Overtaker %in% c("Force India", "Racing Point") ~ "Aston Martin",
      Team_Overtaker %in% c("Alfa Romeo", "Alfa Romeo Racing", "Sauber") ~ "Kick Sauber",
      Team_Overtaker %in% c("AlphaTauri", "Toro Rosso", "RB") ~ "Racing Bulls",
      Team_Overtaker == "Renault" ~ "Alpine",
      TRUE ~ Team_Overtaker),
    Team_Passed = case_when(
      Team_Passed %in% c("Force India", "Racing Point") ~ "Aston Martin",
      Team_Passed %in% c("Alfa Romeo", "Alfa Romeo Racing", "Sauber") ~ "Kick Sauber",
      Team_Passed %in% c("AlphaTauri", "Toro Rosso", "RB") ~ "Racing Bulls",
      Team_Passed == "Renault" ~ "Alpine",
      TRUE ~ Team_Passed),
    Team_Overtaker = str_remove_all(Team_Overtaker, " "),
    Team_Passed = str_remove_all(Team_Passed, " "),
    OvertakingDifficulty = case_when(
      OvertakingDifficulty == "Medium" ~ 1L,
      OvertakingDifficulty == "Good" ~ 2L,
      OvertakingDifficulty == "Difficult" ~ 3L,
      OvertakingDifficulty == "Very Difficult" ~ 4L,
      TRUE ~ as.integer(mean(OvertakingDifficulty))),
    SpeedDescription = case_when(
      SpeedDescription == "Low" ~ 1L,
      SpeedDescription == "Medium" ~ 2L,
      SpeedDescription == "High" ~ 3L,
      SpeedDescription == "Very High" ~ 4L,
      TRUE ~ as.integer(mean(SpeedDescription))),
    Location = recode(Location, 
                      "Yas Island" = "Yas Marina", 
                      "Monte Carlo" = "Monaco",
                      "Miami Gardens" = "Miami"),
    TrackDescription = str_to_lower(paste(Location, S1Description, S2Description, S3Description,
                                          sep = " "))) %>%
  select(-c(AttemptID, Location, S1Description, S2Description, S3Description))

sum(is.na(fin))
#corner kivétele

fin_wo_corners <- fin %>%
  mutate(TrackDescription = str_remove_all(TrackDescription, "\\bcorners?\\b"))

#### Sikeres a végire ####

fin <- fin %>%
  relocate(Sikeres, .after = last_col())

fin_wo_corners <- fin_wo_corners %>%
  relocate(Sikeres, .after = last_col())

#### mentés ####

#cornerekkel
write_parquet(fin, "6_all_overtakes_final_cleaned/final_allvars_2laps.parquet")
write_parquet(fin, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_allvars_2laps.parquet")

#cornerek nélkül
write_parquet(fin_wo_corners, "6_all_overtakes_final_cleaned/final_allvars_wocorners_2laps.parquet")
write_parquet(fin_wo_corners, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_allvars_wo_corners_2laps.parquet")
