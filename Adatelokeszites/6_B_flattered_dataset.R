
####AZ ELŐZÉS KÖRÉT KIVESZEM ####

rm(list=ls())

library(arrow)
library(dplyr)
library(tidyr)
library(openxlsx)

fin <- read_parquet("5_all_overtakes_final/final_dataset.parquet")

sum(is.na(fin))

table(fin$Sikeres)

length(unique(fin$AttemptID))

aranyok <- fin %>%
  group_by(Sikeres) %>%
  summarise(Egyedi_AttemptID = length(unique(AttemptID))) %>%
  print()

#### widing ####

# id_cols = c(AttemptID, Season, Round, EventName, Country, Location, EventDate, BrakingLevel, OvertakingDifficulty, SpeedDescription, Corners, S1Description, S2Description, S3Description, Era, Stratify, Sikeres)

df_static <- fin %>%
  select(AttemptID, Season, Round, EventName, Country, Location, EventDate, 
         BrakingLevel, OvertakingDifficulty, SpeedDescription, Corners,
         S1Description, S2Description, S3Description, Era, Stratify, Sikeres,
         Role, Driver, Team, BaseLap, AirTemp, Humidity, Pressure, Rainfall,
         TrackTemp, WindDirection, WindSpeed) %>%
  distinct() %>%
  pivot_wider(
    names_from = Role,
    values_from = c(Driver, Team, BaseLap),
    names_glue = "{.value}_{Role}"
  )

ertek_valtozok_kor <- c("LapNumber", "LapTimeCalculated", "Position", 
                        "Sector1", "Sector2", "Sector3", "DeltaLapTime", 
                        "DeltaS1Time", "DeltaS2Time", "DeltaS3Time", 
                        "DeltaSector1SessionTime", "DeltaSector2SessionTime", 
                        "DeltaSector3SessionTime", "SpeedI1", "SpeedI2", "SpeedFL", 
                        "SpeedST", "DeltaSpeedI1", "DeltaSpeedI2", "DeltaSpeedFL", 
                        "DeltaSpeedST", "Stint", "CompoundFactor", "FreshTyre", 
                        "TyreLife", "DeltaTyreLife", "DRS_OTSector", "DRS_Lap")

df_laps <- fin %>%
  select(AttemptID, Role, all_of(ertek_valtozok_kor)) %>%
  group_by(AttemptID, Role) %>%
  arrange(LapNumber, .by_group = TRUE) %>%
  mutate(Lap_Index = row_number()) %>%
  # filter(Lap_Index <= 3) %>%
  ungroup() %>%
  pivot_wider(
    names_from = c(Role, Lap_Index),
    values_from = all_of(ertek_valtozok_kor),
    names_glue = "{.value}_{Role}_{Lap_Index}"
  ) %>%
  select(-matches("^Delta.*_Passed_"))

wide_final <- df_static %>%
  left_join(df_laps, by = "AttemptID")

colSums(is.na(wide_final))

wf_colnames <- as.data.frame(sort(colnames(wide_final)))
write.xlsx(wf_colnames, "B_verzio_szures_elotti_colnames.xlsx")


sum(is.na(wide_final$DRS_OTSector_Overtaker_3))
sum(is.na(wide_final$DRS_OTSector_Passed_3))

wide_final <- wide_final %>%
  rename(DRS_OTSector_Overtaker = DRS_OTSector_Overtaker_3,
         DRS_OTSector_Passed = DRS_OTSector_Passed_3)

sum(is.na(wide_final$DRS_OTSector_Overtaker))
sum(is.na(wide_final$DRS_OTSector_Passed))
class(wide_final$DRS_OTSector_Overtaker)
class(wide_final$DRS_OTSector_Passed)

wide_final <- wide_final %>%
  mutate(DRS_OTSector_Overtaker = case_when(
    is.na(DRS_OTSector_Overtaker) & (abs(DeltaSector3SessionTime_Overtaker_2) < 1) ~ 1L,
    is.na(DRS_OTSector_Overtaker) & (abs(DeltaSector3SessionTime_Overtaker_2) > 1) ~ 0L,
    TRUE ~ DRS_OTSector_Overtaker),
    DRS_OTSector_Passed = case_when(
      is.na(DRS_OTSector_Passed) ~ sample(c(1L, 0L), size = n(), replace = TRUE, prob = c(0.33, 0.67)),
      TRUE ~ DRS_OTSector_Passed))

vegzodesek <- paste0("_", 3:12)

wide_clean <- wide_final %>%
  select(-ends_with(vegzodesek))

wfc_colnames <- as.data.frame(sort(colnames(wide_clean)))
write.xlsx(wfc_colnames, "B_verzio_szures_utani1_colnames.xlsx")

wide_clean2 <- wide_clean %>%
  select(-c(CompoundFactor_Overtaker_2, CompoundFactor_Passed_2, DRS_OTSector_Overtaker_1,
            DRS_OTSector_Overtaker_2, DRS_OTSector_Passed_1, DRS_OTSector_Passed_2,
            FreshTyre_Overtaker_2, FreshTyre_Passed_2, LapNumber_Passed_1, LapNumber_Passed_2,
            Position_Overtaker_2, Position_Passed_2, Stint_Overtaker_2, Stint_Passed_2, Era,
            Round, EventName, EventDate, Country)) %>%
  select(-starts_with("Sector")) %>%
  rename(BaseLap = BaseLap_Overtaker,
         LapNumber1 = LapNumber_Overtaker_1,
         LapNumber2 = LapNumber_Overtaker_2,
         Compound_Overtaker = CompoundFactor_Overtaker_1,
         Compound_Passed = CompoundFactor_Passed_1,
         FreshTyre_Overtaker = FreshTyre_Overtaker_1,
         FreshTyre_Passed = FreshTyre_Passed_1,
         Position_Overtaker = Position_Overtaker_1,
         Position_Passed = Position_Passed_1,
         Stint_Overtaker = Stint_Overtaker_1,
         Stint_Passed = Stint_Passed_1)

colnames(wide_clean2)

colSums(is.na(wide_clean2))

wide_clean2[is.na(wide_clean2)] <- 0L

table(wide_clean2$Sikeres)

9148/2
any(duplicated(fin$AttemptID))

# Teljesen megegyező sorok száma
sum(duplicated(wide_clean2))

teljes_duplikatumok <- wide_clean2 %>% #ez üres
  group_by(across(everything())) %>%
  filter(n() > 1) %>%
  ungroup()

duplicated_rows <- which(duplicated(wide_clean2$AttemptID))
head(duplicated_rows)

attempt_duplikatumok <- wide_clean2 %>%
  group_by(AttemptID) %>%
  filter(n() > 1) %>%
  ungroup()

table(attempt_duplikatumok$Sikeres) #csak ennél van :)

#minden id-ból csak az elsőt megtartani
#wcuniq <- wide_clean2 %>%
  arrange(AttemptID) %>%   # vagy pl. arrange(AttemptID, timestamp)
  group_by(AttemptID) %>%
  slice(1) %>%
  ungroup()

#mivel az egyetlen különbség az idójárási adatokban van, megnézem a véeltelnszerű
#kiválasztást

wcuniq <- wide_clean2 %>%
  group_by(AttemptID) %>%
  slice_sample(n = 1) %>% 
  ungroup()

table(wcuniq$Sikeres)

sum(is.na(wcuniq))

colSums(is.na(wcuniq))

wcuniq <- wcuniq %>%
  relocate(Sikeres, .after = last_col())

write_parquet(wcuniq, "5_all_overtakes_final/final_dataset_flattered.parquet")
#write_parquet(wide_clean2, "5_all_overtakes_final/final_dataset_flattered2.parquet")

#véletlen minta 

minta <- wcuniq %>%
  sample_frac(0.01)

write.xlsx(minta, "final_dataset_minta.xlsx")
