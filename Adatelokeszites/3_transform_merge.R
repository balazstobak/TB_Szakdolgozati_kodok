#-------------------------------------------------------------------------------

#Ebben
#1. mégnézem a sikeres és sikertelenek struktúráját
#2. megszabadulok a segédváltozóktól
#3. egységes struktúrába rendezem a két objekt típust
#4. külön kimentem ezeket is
#5. összemergelem egy nagy objektté őket
#-------------------------------------------------------------------------------


rm(list=ls())

library(dplyr)
library(arrow)
library(lubridate)
library(purrr)
library(stringr)
library(writexl)

#### Adatbázisok megnyitása ####

ds_succ <- open_dataset("2_1_all_overtakes_weather/sikeres")
ds_fail <- open_dataset("2_1_all_overtakes_weather/sikertelen")

ds_succ %>%
  group_by(Season, Round) %>%
  count(OvertakeID) %>%
  filter(n > 6) %>%
  ungroup() %>%
  collect()

ds_fail %>%
  group_by(Season, Round) %>%
  count(FailedID) %>%
  filter(n > 6) %>%
  ungroup() %>%
  collect()

# Hány unique Weather_Time van OvertakeID + Role + LapNumber kombinációnként?
ds_succ %>%
  group_by(Season, Round, OvertakeID, Role, LapNumber) %>%
  count() %>%
  filter(n > 1) %>%
  ungroup() %>%
  collect()

ds_succ %>%
  group_by(Season, Round, OvertakeID, Role) %>%
  count() %>%
  filter(n > 3) %>%
  collect()
#### Változók ellenőrzése + kimentése excelbe ####

#segédfüggvény, ami kinyeri a sémából a neveket és típusokat egy táblázatba
get_schema <- function(arrow_schema) {
  tibble(
    Valtozo_Neve = names(arrow_schema),
    # A fields listából kiszedjük az adattípusok szöveges megfelelőjét
    Tipus = sapply(arrow_schema$fields, function(x) x$type$ToString())
  )
}

#adattömbök ellenőrzése
df_suc_schema <- get_schema(ds_succ$schema)
df_fail_schema <- get_schema(ds_fail$schema)

schema_osszehasonlitas <- full_join(
  df_suc_schema,
  df_fail_schema,
  by = "Valtozo_Neve",
  suffix = c("_Sikeres", "_Sikertelen")
)

write_xlsx(schema_osszehasonlitas, "2_1_all_overtakes_weather/sikeres_sikertelen_valtozok_osszehasonlitas.xlsx")

#### Transzformációk ####

#ID-k:
  #minden ID megkapja: season_round_eredetiID


transform_ds <- function(dataset, id_col_name) {
  dataset |>
    mutate(
      AttemptID = paste0(Season, "_", Round, "_", Sikeres, "_", .data[[id_col_name]])
      #AttemptID = as.integer((Season * 100000) + (Round * 1000) + (Sikeres * 1000) + .data[[id_col_name]])
    ) |>
    select(
      AttemptID, Season, Round, Driver, Team, Role, BaseLap, LapNumber, LapTimeCalculated,
      Position, Sector1, Sector2, Sector3, DeltaLapTime, DeltaS1Time, DeltaS2Time,
      DeltaS3Time, DeltaSector1SessionTime, DeltaSector2SessionTime, DeltaSector3SessionTime,
      SpeedI1, SpeedI2, SpeedFL, SpeedST, DeltaSpeedI1, DeltaSpeedI2, DeltaSpeedFL,
      DeltaSpeedST, Stint, CompoundFactor, FreshTyre, TyreLife, DeltaTyreLife,
      DRS_OTSector, DRS_Lap, EventName, Country, Location, EventDate, Weather_Time, AirTemp,
      Humidity,Pressure,Rainfall,TrackTemp,WindDirection,WindSpeed, Sikeres)
}

# Futtassuk rá a függvényt mindkét lusta adathalmazra
ds_succ_clean <- transform_ds(ds_succ, "OvertakeID")
ds_fail_clean <- transform_ds(ds_fail, "FailedID")

#### Mentés 1 ####

write_parquet(collect(ds_succ_clean), "3_all_overtakes_to_merge/successful_clean.parquet")
write_parquet(collect(ds_fail_clean), "3_all_overtakes_to_merge/failed_clean.parquet")

#### Mergelés ####

# Visszaolvassuk az immár tiszta parquet fájlokat a memóriába
df_succ_final <- read_parquet("3_all_overtakes_to_merge/successful_clean.parquet")
df_fail_final <- read_parquet("3_all_overtakes_to_merge/failed_clean.parquet")

# Egyesítés egy nagy adatkeretté
merged_overtakes <- bind_rows(df_succ_final, df_fail_final)

#### Mentés 2 ####
write_parquet(merged_overtakes, "4_all_overtakes_merged_imputated/final_dataset.parquet")

colSums(is.na(merged_overtakes))


