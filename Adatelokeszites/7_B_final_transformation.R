#erős multikollinearistás volt sok változó között, ezért ebben a scriptben
#kiszedem az összes ilyet

#Logreghez mindenképp ezt használom, GXB-hez és MLP-hez valszeg a 7_B_allvariables...
#scriptet -->


rm(list=ls())

library(arrow)
library(dplyr)
library(stringr)
library(openxlsx)
library(tidytext)

final <- read_parquet("5_all_overtakes_final/final_dataset_flattered.parquet")

cn <- as.list(colnames(final))

#### ellenőrzések ####

teamo <- as.data.frame(sort(unique(final$Team_Overtaker)))
teamp <- as.data.frame(sort(unique(final$Team_Passed)))

teamo == teamp

write.xlsx(teamo, "constructors.xlsx")

unique(final$BrakingLevel)

unique(final$OvertakingDifficulty)

unique(final$SpeedDescription)

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
  rename(DeltaLapTime_1 = DeltaLapTime_Overtaker_1,
         DeltaLapTime_2= DeltaLapTime_Overtaker_2,
         DeltaS1Time_1 = DeltaS1Time_Overtaker_1,
         DeltaS1Time_2 = DeltaS1Time_Overtaker_2,
         DeltaS2Time_1 = DeltaS2Time_Overtaker_1,
         DeltaS2Time_2 = DeltaS2Time_Overtaker_2,
         DeltaS3Time_1 = DeltaS3Time_Overtaker_1,
         DeltaS3Time_2 = DeltaS3Time_Overtaker_2,
         TyreLife_Overtaker = TyreLife_Overtaker_2,
         TyreLife_Passed = TyreLife_Passed_2,
         TargetPosition = Position_Passed) %>%
  select(-c(AttemptID, Location, S1Description, S2Description, S3Description,
            BaseLap_Passed, LapNumber1, LapNumber2, TyreLife_Passed_1, TyreLife_Overtaker_1,
            Position_Overtaker))

cnfin <- as.list(colnames(fin))

#### corner/s eltávolítása ####

#azt látom, hogy a corners és corner szavak összesen ~11500 alkalommal fordulnak elő
#ezeket én:
  #meghagyom, és használom a TF-IDF ngram_range-ét --> fin_clean_2laps.parquet
  #kiszedem a corner és corners szavakat --> fin_clean_wo_corners_2laps.parquet 

fin_wo_corners <- fin %>%
  mutate(TrackDescription = str_remove_all(TrackDescription, "\\bcorners?\\b"))

#### Sikeres a végire ####

fin <- fin %>%
  relocate(Sikeres, .after = last_col())

fin_wo_corners <- fin_wo_corners %>%
  relocate(Sikeres, .after = last_col())

as.list(sort(colnames(fin)))

#### alapadatok kiszűrése --> csak a delták megtartása ####

fin_delta <- fin %>%
  select(-c(LapTimeCalculated_Overtaker_1, LapTimeCalculated_Overtaker_2, 
            LapTimeCalculated_Passed_1, LapTimeCalculated_Passed_2,
            SpeedI1_Overtaker_1, SpeedI1_Overtaker_2, SpeedI1_Passed_1, SpeedI1_Passed_2,
            SpeedI2_Overtaker_1, SpeedI2_Overtaker_2, SpeedI2_Passed_1,SpeedI2_Passed_2,
            SpeedFL_Overtaker_1, SpeedFL_Overtaker_2, SpeedFL_Passed_1, SpeedFL_Passed_2,
            SpeedST_Overtaker_1, SpeedST_Overtaker_2, SpeedST_Passed_1, SpeedST_Passed_2,
            DeltaTyreLife_Overtaker_1, TyreLife_Overtaker, TyreLife_Passed)) %>%
  rename(DeltaTyreLife = DeltaTyreLife_Overtaker_2,
         DeltaSector1SessionTime_1 = DeltaSector1SessionTime_Overtaker_1,
         DeltaSector1SessionTime_2 = DeltaSector1SessionTime_Overtaker_2,
         DeltaSector2SessionTime_1 = DeltaSector2SessionTime_Overtaker_1,
         DeltaSector2SessionTime_2 = DeltaSector2SessionTime_Overtaker_2,
         DeltaSector3SessionTime_1 = DeltaSector3SessionTime_Overtaker_1,
         DeltaSector3SessionTime_2 = DeltaSector3SessionTime_Overtaker_2,
         DeltaSpeedFL_1 = DeltaSpeedFL_Overtaker_1,
         DeltaSpeedFL_2 = DeltaSpeedFL_Overtaker_2,
         DeltaSpeedI1_1 = DeltaSpeedI1_Overtaker_1,
         DeltaSpeedI1_2 = DeltaSpeedI1_Overtaker_2,
         DeltaSpeedI2_1 = DeltaSpeedI2_Overtaker_1,
         DeltaSpeedI2_2 = DeltaSpeedI2_Overtaker_2,
         DeltaSpeedST_1 = DeltaSpeedST_Overtaker_1,
         DeltaSpeedST_2 = DeltaSpeedST_Overtaker_2)

cndelta <- as.list(colnames(fin_delta))

fin_delta_wo_corners <- fin_wo_corners %>%
  select(-c(LapTimeCalculated_Overtaker_1, LapTimeCalculated_Overtaker_2, 
            LapTimeCalculated_Passed_1, LapTimeCalculated_Passed_2,
            SpeedI1_Overtaker_1, SpeedI1_Overtaker_2, SpeedI1_Passed_1, SpeedI1_Passed_2,
            SpeedI2_Overtaker_1, SpeedI2_Overtaker_2, SpeedI2_Passed_1,SpeedI2_Passed_2,
            SpeedFL_Overtaker_1, SpeedFL_Overtaker_2, SpeedFL_Passed_1, SpeedFL_Passed_2,
            SpeedST_Overtaker_1, SpeedST_Overtaker_2, SpeedST_Passed_1, SpeedST_Passed_2,
            DeltaTyreLife_Overtaker_1, TyreLife_Overtaker, TyreLife_Passed)) %>%
  rename(DeltaTyreLife = DeltaTyreLife_Overtaker_2,
         DeltaSector1SessionTime_1 = DeltaSector1SessionTime_Overtaker_1,
         DeltaSector1SessionTime_2 = DeltaSector1SessionTime_Overtaker_2,
         DeltaSector2SessionTime_1 = DeltaSector2SessionTime_Overtaker_1,
         DeltaSector2SessionTime_2 = DeltaSector2SessionTime_Overtaker_2,
         DeltaSector3SessionTime_1 = DeltaSector3SessionTime_Overtaker_1,
         DeltaSector3SessionTime_2 = DeltaSector3SessionTime_Overtaker_2,
         DeltaSpeedFL_1 = DeltaSpeedFL_Overtaker_1,
         DeltaSpeedFL_2 = DeltaSpeedFL_Overtaker_2,
         DeltaSpeedI1_1 = DeltaSpeedI1_Overtaker_1,
         DeltaSpeedI1_2 = DeltaSpeedI1_Overtaker_2,
         DeltaSpeedI2_1 = DeltaSpeedI2_Overtaker_1,
         DeltaSpeedI2_2 = DeltaSpeedI2_Overtaker_2,
         DeltaSpeedST_1 = DeltaSpeedST_Overtaker_1,
         DeltaSpeedST_2 = DeltaSpeedST_Overtaker_2)

#### mentés ####

#cornerekkel
write_parquet(fin, "6_all_overtakes_final_cleaned/final_clean_2laps.parquet")
write_parquet(fin, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_clean_2laps.parquet")

#cornerek NÉLKÜL
write_parquet(fin_wo_corners, "6_all_overtakes_final_cleaned/final_clean_wo_corners_2laps.parquet")
write_parquet(fin_wo_corners, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_clean_wo_corners_2laps.parquet")

#csak deltákkal
write_parquet(fin_delta, "6_all_overtakes_final_cleaned/final_clean_delta_2laps.parquet")
write_parquet(fin_delta, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_clean_delta_2laps.parquet")

#deltákkal corner nélkül
write_parquet(fin_delta_wo_corners, "6_all_overtakes_final_cleaned/final_delta_wocorners_2laps.parquet")
write_parquet(fin_delta_wo_corners, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_delta_wocorners_2laps.parquet")

#### véletlen minta kimentése ####

#fin_tiszitott1 <- as.data.frame(sample_n(fin, 50))
#write.xlsx(fin_tiszitott1, "final_tisztitott.xlsx")

#### Driver előfordulások ####

csek <- read_parquet("6_all_overtakes_final_cleaned/final_clean.parquet")

length(unique(fin$Team_Overtaker))

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

sort(unique(fin_delta$TrackDescription))
