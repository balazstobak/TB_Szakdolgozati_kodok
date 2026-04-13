rm(list=ls())

library(arrow)
library(dplyr)
library(stringr)
library(openxlsx)
library(tidytext)

#final <- read_parquet("5_all_overtakes_final/final_flattered_3lap.parquet")
final <- read_parquet("5_all_overtakes_final/final_dataset_flattered.parquet")

#### csapatok áttekintése ####

teamo <- as.data.frame(sort(unique(final$Team_Overtaker)))
teamp <- as.data.frame(sort(unique(final$Team_Passed)))

teamo == teamp

write.xlsx(teamo, "constructors.xlsx")

#### track descriptions ####

unique(final$BrakingLevel)

unique(final$OvertakingDifficulty)

unique(final$SpeedDescription)

#### transzformációk ####

fin <- final %>%
  mutate(
    Rainfall = if_else(Rainfall == TRUE, 1L, 0L),
    FreshTyre_Overtaker_3 = if_else(FreshTyre_Overtaker_3 == TRUE, 1L, 0L),
    FreshTyre_Passed_3 = if_else(FreshTyre_Passed_3 == TRUE, 1L, 0L),
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
                      "Monte Carlo" = "Monaco"),
    TrackDescription = str_to_lower(paste(Location, S1Description, S2Description, S3Description,
                                          sep = " "))) %>%
  select(-c(AttemptID, Round, Era, Location, S1Description, S2Description, S3Description))

#### trackdescription szógyakoriságok locationnel ####

word_freq_df <- fin %>%
  filter(!is.na(TrackDescription)) %>%
  unnest_tokens(Szo, TrackDescription) %>%
  count(Szo, name = "Gyakorisag", sort = TRUE)

total_unique_words <- nrow(word_freq_df)
cat("Összesen", total_unique_words, "különböző szó található a TrackDescription oszlopban.\n")

write.xlsx(word_freq_df, "7_final_tr_szogyakorisag.xlsx")

# azt látom, hogy a corners és corner szavak összes ~11500 alkalommal fordulnak elő
# ezeket én:
  #meghagyom, és használom a TF-IDF ngram_range-ét --> fin_clean_3laps.parquet
  #kiszedem a corner és corners szavakat --> fin_clean_wo_corners_3laps.parquet 

fin_wo_corners <- fin %>%
  mutate(TrackDescription = str_remove_all(TrackDescription, "\\bcorners?\\b"))

#### sikeres a végére ####

fin <- fin %>%
  relocate(Sikeres, .after = last_col())

fin_wo_corners <- fin_wo_corners %>%
  relocate(Sikeres, .after = last_col())

#### mentés ####

#cornerekkel
write_parquet(fin, "6_all_overtakes_final_cleaned/final_clean_3laps.parquet")
write_parquet(fin, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_clean_3laps.parquet")

#cornerek NÉLKÜL
write_parquet(fin_wo_corners, "6_all_overtakes_final_cleaned/final_clean_wo_corners_3laps.parquet")
write_parquet(fin_wo_corners, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_clean_wo_corners_3laps.parquet")



unique(fin$Rainfall)
unique(fin$FreshTyre_Overtaker_3)
unique(fin$FreshTyre_Passed_3)
unique(fin$Team_Overtaker)
unique(fin$Team_Passed)
unique(fin$Team_Overtaker)
unique(fin$Team_Passed)
unique(fin$OvertakingDifficulty)
unique(fin$SpeedDescription)
unique(fin$TrackDescription)

fin_tiszitott1 <- as.data.frame(sample_n(fin, 50))
write.xlsx(fin_tiszitott1, "final_tisztitott_3koros.xlsx")

#### Driver előfordulások ####

csek <- read_parquet("6_all_overtakes_final_cleaned/final_clean.parquet")

length(unique(csek$Team_Overtaker))

oter <- sort(length(unique(csek$Driver_Overtaker)))
passed <- sort(length(unique(csek$Driver_Passed)))
oter == passed

driver_gyak <- csek %>%
  group_by(Driver_Overtaker) %>%
  summarise(
    Gyakorisag = n(),
    Szazalek = (n() / nrow(csek)) * 100
  ) %>%
  arrange(Gyakorisag) %>%
  mutate(Kumulativ_Szazalek = cumsum(Szazalek))

sum(driver_gyak$Szazalek)

length(unique(csek$Location))

write.xlsx(driver_gyak, "driver_gyakorisagok_vegsoben.xlsx")

#### Location eldobása ####

fin <- fin %>%
  select(-Location)

"Location" %in% colnames(fin)

write_parquet(fin, "6_all_overtakes_final_cleaned/final_clean.parquet")
write_parquet(fin, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_clean.parquet")
