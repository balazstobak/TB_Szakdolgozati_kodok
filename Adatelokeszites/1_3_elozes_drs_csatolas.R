
rm(list=ls())

#### csomagok ####

library(dplyr)
library(arrow)
library(duckdb)

#### remove az előző obejkteket ####

unique(ot$Season)
rm(ot, cd, ot_drs, filtered, season, con, query)

#### Szezononkénti OT parquetek beolvasása ####

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2018_success.parquet")
season <- 2018L
cd <- open_dataset("2018/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2019_success.parquet")
season <- 2019L
cd <- open_dataset("2019/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2020_success.parquet")
season <- 2020L
cd <- open_dataset("2020/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2021_success.parquet")
season <- 2021L
cd <- open_dataset("2021/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2022_success.parquet")
season <- 2022L
cd <- open_dataset("2022/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2023_success.parquet")
season <- 2023L
cd <- open_dataset("2023/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2024_success.parquet")
season <- 2024L
cd <- open_dataset("2024/car_data")

ot <- read_parquet("1_all_overtakes_wodrs/sikeres/2025_success.parquet")
season <- 2025L
cd <- open_dataset("2025/car_data")

#### Intervallok difftimmá alakítása

ot <- ot %>%
  mutate(OvertakeIntervall_Bottom = as.difftime(OvertakeIntervall_Bottom, units = "secs"),
         OvertakeIntervall_Upper = as.difftime(OvertakeIntervall_Upper, units = "secs"),
         LST = as.difftime(LapStartTime, units = "secs"),
         Time = as.difftime(Time, units = "secs"))

#### Kapcsolat létrehozása és táblák regisztrálása ####

con <- dbConnect(duckdb::duckdb())
duckdb_register(con, "ot_table", ot)
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
        AND c.SessionTime >= o.OvertakeIntervall_Bottom 
        AND c.SessionTime <= o.OvertakeIntervall_Upper
        AND c.DRS BETWEEN 10 AND 14
    ) THEN 1 ELSE 0 END AS DRS_OTSector,
    
    -- 2. ÚJ: BaseLap - 1. kör DRS ellenőrzése (nem BaseLap)
    CASE WHEN EXISTS (
      SELECT 1 
      FROM cd_table c
      WHERE c.Round = o.Round        
        AND c.Driver = o.Driver      
        AND c.SessionTime >= o.LST
        AND c.SessionTime <= o.Time
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
  select(OvertakeID, Driver, Role, BaseLap, LapNumber, DRS_OTSector, DRS_Lap)

table(filtered$DRS_OTSector[filtered$Role == "Overtaker"])

#### Mentés ####

#mappa <- "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/adatok/2_all_overtakes_drs/sikeres"

write_parquet(ot_drs, sink = file.path(mappa, paste0(season, "_drs_success.parquet")))
