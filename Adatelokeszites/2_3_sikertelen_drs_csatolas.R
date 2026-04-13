rm(list=ls())

#### csomagok ####

library(dplyr)
library(arrow)
library(duckdb)

#### remove az előző obejkteket ####

unique(ot$Season)
rm(ot, otint, cd, ot_drs, filtered, season, con, query)

#### Szezononkénti OT parquetek beolvasása ####

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2018_failed.parquet")
season <- 2018L
cd <- open_dataset("2018/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2019_failed.parquet")
season <- 2019L
cd <- open_dataset("2019/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2020_failed.parquet")
season <- 2020L
cd <- open_dataset("2020/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2021_failed.parquet")
season <- 2021L
cd <- open_dataset("2021/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2022_failed.parquet")
season <- 2022L
cd <- open_dataset("2022/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2023_failed.parquet")
season <- 2023L
cd <- open_dataset("2023/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2024_failed.parquet")
season <- 2024L
cd <- open_dataset("2024/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikertelen/2025_failed.parquet")
season <- 2025L
cd <- open_dataset("2025/car_data")

#### Legközelebbi ponton volt-e DRS használat ####

#BaseLap-ban voltak egymáshoz a legközelebb
#megnézem, melyik szektorban voltak a delta a legkisebb
#ebben a szektorban fogom megnézni, hogy volt-e DRS használat

otint <- ot %>%
  group_by(Round, Location, FailedID) %>%
  mutate(
    diff_S1 = abs(DeltaSector1SessionTime - DeltaSessionTime),
    diff_S2 = abs(DeltaSector2SessionTime - DeltaSector1SessionTime),
    diff_S3 = abs(DeltaSector3SessionTime - DeltaSector2SessionTime),
    min_diff = pmin(diff_S1, diff_S2, diff_S3, na.rm = TRUE),
    TryIntervall_Bottom = as.difftime(case_when(
      LapNumber == BaseLap & min_diff == diff_S1 ~ LapStartTime,
      LapNumber == BaseLap & min_diff == diff_S2 ~ Sector1SessionTime,
      LapNumber == BaseLap & min_diff == diff_S3 ~ Sector2SessionTime,
      TRUE ~ 0), units = "secs"),
    TryIntervall_Upper = as.difftime(case_when(
      LapNumber == BaseLap & min_diff == diff_S1 ~ Sector1SessionTime,
      LapNumber == BaseLap & min_diff == diff_S2 ~ Sector2SessionTime,
      LapNumber == BaseLap & min_diff == diff_S3 ~ Sector3SessionTime,
      TRUE ~ 0), units = "secs"),
    LST = as.difftime(LapStartTime, units = "secs"),
    LFT = as.difftime(Time, units = "secs"))%>%
  ungroup()%>%
  select(-diff_S1, -diff_S2, -diff_S3, -min_diff)

#### Kapcsolat létrehozása és táblák regisztrálása ####

con <- dbConnect(duckdb::duckdb())
duckdb_register(con, "ot_table", otint)
duckdb_register_arrow(con, "cd_table", cd)

#fennáll-e a kapcsolat
dbIsValid(con)

#### DRS_OTSector és DRS_Lap ####

query <- "
  SELECT 
    o.*,
    
    -- 1. Eredeti: BaseLap DRS ellenőrzése
    CASE WHEN EXISTS (
      SELECT 1 
      FROM cd_table c
      WHERE c.Round = o.Round        
        AND c.Driver = o.Driver      
        AND c.SessionTime >= o.TryIntervall_Bottom 
        AND c.SessionTime <= o.TryIntervall_Upper
        AND c.DRS BETWEEN 10 AND 14
    ) THEN 1 ELSE 0 END AS DRS_OTSector,
    
    -- 2. ÚJ: BaseLap - 1. kör DRS ellenőrzése (nem BaseLap)
    CASE WHEN EXISTS (
      SELECT 1 
      FROM cd_table c
      WHERE c.Round = o.Round        
        AND c.Driver = o.Driver      
        AND c.SessionTime >= o.LST
        AND c.SessionTime <= o.LFT
        AND c.DRS BETWEEN 10 AND 14
    ) THEN 1 ELSE 0 END AS DRS_Lap
    
  FROM ot_table o
"

#### Eredmény letöltése ####

ot_drs <- dbGetQuery(con, query)

#### Kapcsolat lezárása ####

dbDisconnect(con, shutdown = TRUE)

#fennáll-e a kapcsolat
dbIsValid(con)

#### gyors csekk ####

filtered <- ot_drs %>%
  filter(BaseLap == LapNumber, Role == "Overtaker") %>%
  select(FailedID, Driver, Role, BaseLap, LapNumber, DRS_OTSector, DRS_Lap)

table(filtered$DRS_OTSector[filtered$Role == "Overtaker"])

unique(ot_drs$DRS_OTSector)

#### Mentés ####

#mappa <- "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/adatok/2_all_overtakes_drs/sikertelen"

write_parquet(ot_drs, sink = file.path(mappa, paste0(season, "_final_failed.parquet")))
