rm(list=ls())

library(dplyr)
library(tidyr)
library(purrr)
library(arrow)
library(lubridate)
library(stringr)
library(zoo)

#### Függvény ####

elozes_fgny <- function(laps) {
  
  laps <- laps %>%
    mutate(across(
      c(Time, LapTime, Sector1Time, Sector2Time, Sector3Time, LapStartTime, 
        Sector1SessionTime, Sector2SessionTime, Sector3SessionTime
        ,PitInTime, PitOutTime), 
      ~ as.numeric(., units = "secs")
    )) %>%
    select(-c(IsPersonalBest, Deleted, DeletedReason, DeletedReason, IsAccurate,
            FastF1Generated))
  
  laps1 <- laps %>%
    mutate(LapTimeCalculated =
      if_else(is.na(LapTime), Sector1Time + Sector2Time + Sector3Time, LapTime),
      TyreLife = na.approx(TyreLife, na.rm = FALSE),
      SpeedI1  = na.approx(SpeedI1, na.rm = FALSE),
      SpeedI2  = na.approx(SpeedI2, na.rm = FALSE),
      SpeedST  = na.approx(SpeedST, na.rm = FALSE),
      SpeedFL  = na.approx(SpeedFL, na.rm = FALSE)) %>%
    fill(TyreLife, SpeedFL, .direction = "downup")
  
  rm(laps)
  
  laps2 <- laps1 %>%
    mutate(
      Sector1 = if_else(is.na(Sector1Time),
                                    LapTimeCalculated - Sector2Time - Sector3Time, Sector1Time),
      Sector2 = if_else(is.na(Sector2Time),
                                    LapTimeCalculated - Sector1Time - Sector3Time, Sector2Time),
      Sector3 = if_else(is.na(Sector3Time),
                                    LapTimeCalculated - Sector1Time - Sector2Time, Sector3Time)
    )
  
  rm(laps1)
  
  laps2 <- laps2 %>%
    group_by(Driver) %>% 
    mutate(
      LapTime_tmp = coalesce(LapTime, Sector1Time + Sector2Time + Sector3Time),
      Sector1_tmp = coalesce(Sector1Time, LapTime_tmp - Sector2Time - Sector3Time),
      Sector2_tmp = coalesce(Sector2Time, LapTime_tmp - Sector1_tmp - Sector3Time),
      Sector3_tmp = coalesce(Sector3Time, LapTime_tmp - Sector1_tmp - Sector2_tmp),
      LapTimeCalculated = 
        coalesce(LapTime_tmp, median(LapTime_tmp, na.rm = TRUE)),
      Sector1 = 
        coalesce(Sector1_tmp, median(Sector1_tmp, na.rm = TRUE)),
      Sector2 =
        coalesce(Sector2_tmp, median(Sector2_tmp, na.rm = TRUE)),
      Sector3 =
        coalesce(Sector3_tmp, median(Sector3_tmp, na.rm = TRUE))) %>%
    select(-LapTime_tmp, -Sector1_tmp, -Sector2_tmp, -Sector3_tmp) %>%
    ungroup()
  
  laps3 <- laps2 %>%
    arrange(Driver, LapNumber) %>%
    group_by(Driver) %>%
    mutate(
      PitEntryTime = lead(LapStartTime) - PitInTime,
      PitExitTime  = PitOutTime - LapStartTime,
      PitTime      = PitEntryTime + lead(PitExitTime),
      PitInLap     = if_else(!is.na(PitInTime),  1L, 0L), #1= box BE
      PitOutLap    = if_else(!is.na(PitOutTime), 1L, 0L), #1= box KI
      PitOutLagLap = if_else(lag(PitOutLap, default = 0L) == 1L, 1L, 0L),
      Pit          = if_else(PitInLap == 1L | PitOutLap == 1L, 1L, 0L), #1 = box KI vagy BE
      PitSafe = coalesce(Pit, 0),
      Stint = if_else(is.na(Stint), cumsum(lag(PitSafe, default = 0)) + 1L, Stint)
    ) %>%
    select(-PitSafe) %>%
    ungroup()
  
  rm(laps2)
  
  max_lap_number <- max(laps3$LapNumber, na.rm = TRUE)
  
  laps4 <- laps3 %>%
    group_by(Driver) %>%
    mutate(DNF = if_else(max(LapNumber) >= (max_lap_number - 1L), 0L, max(LapNumber))) %>%
    ungroup()
  
  rm(laps3)
  rm(max_lap_number)
  
  laps5 <- laps4 %>%
    arrange(Driver, LapNumber) %>%
    group_by(Driver) %>%
    mutate(
      PrevLapPosition = lag(Position),
      PositionChange  = Position - PrevLapPosition
    ) %>%
    ungroup()
  
  rm(laps4)
  
  laps6 <- laps5 %>%
    group_by(LapNumber) %>%
    mutate(Flag = as.integer(TrackStatus != '1')) %>%
    ungroup()
  
  rm(laps5)
  
  laps7 <- laps6 %>%
    group_by(LapNumber) %>%
    mutate(OTable = as.integer(if_else(
      Pit == 0 & grepl("1", TrackStatus) & !is.na(LapTimeCalculated), 1, 0))) %>%
    ungroup()
  
  rm(laps6)
  
  lap_context <- laps7 %>%
    filter(!is.na(LapNumber), !is.na(PrevLapPosition)) %>% #maradhat a LN, mert LN_num is ua lesz mindig sajnos
    select(LapNumber, Driver, Position, PrevLapPosition, PitInLap, PitOutLap, PitOutLagLap, DNF) %>%
    rename(
      PassedDriver = Driver,
      PassedDriverCurrPos = Position,
      PassedDriverPrevPos = PrevLapPosition,
      PassedDriverPitIn = PitInLap,
      PassedDriverPitOut = PitOutLap,
      PassedDriverPitOutLag = PitOutLagLap,
      PassedDriverDNF = DNF
    )
  
  laps8 <- laps7 %>%
    left_join(
      lap_context, 
      join_by(
        LapNumber == LapNumber,
        PrevLapPosition > PassedDriverPrevPos, 
        Position <= PassedDriverPrevPos 
      )
    ) %>%
    filter(is.na(PassedDriver) | PassedDriver != Driver) %>% 
    mutate(
      PassedDriverPitIn     = if_else(is.na(PassedDriverPitIn),     0L, PassedDriverPitIn),
      PassedDriverPitOut    = if_else(is.na(PassedDriverPitOut),    0L, PassedDriverPitOut),
      PassedDriverPitOutLag = if_else(is.na(PassedDriverPitOutLag), 0L, PassedDriverPitOutLag),
      PassedDriverDNF       = if_else(is.na(PassedDriverDNF),       0L, PassedDriverDNF)
    )
  
  rm(laps7)
  
  ##### Detektálás ####
  
  laps9 <- laps8 %>%
    mutate(
      Overtake = case_when(
        !is.na(PassedDriver) & 
          PositionChange < 0 & 
          LapNumber > 1 & 
          OTable == 1L &
          PitInLap == 0L &  
          PitOutLap == 0L &
          PassedDriverPitIn == 0L & 
          PassedDriverPitOut == 0L & 
          PassedDriverPitOutLag == 0L &
          PassedDriverDNF == 0L & 
          PassedDriverCurrPos >= PassedDriverPrevPos
        ~ 1L,
        TRUE ~ 0L
      )
    )
  
  rm(laps8)
  
  overtakes <- laps9 %>%
    filter(Overtake == 1L) %>%
    mutate(OvertakeID = row_number()) %>% 
    select(OvertakeID, LapNumber, Driver, PassedDriver)
  
  laps_clean <- laps9 %>%
    select(-Overtake,
           -PassedDriver,
           -PassedDriverPitIn,
           -PassedDriverPitOut,
           -PassedDriverPitOutLag,
           -PassedDriverDNF) %>%
    distinct(Driver, LapNumber, .keep_all = TRUE)
  
  rm(laps9)
  
  overtake_context <- overtakes %>%
    rename(BaseLap = LapNumber) %>%
    pivot_longer(
      cols = c(Driver, PassedDriver), 
      names_to = "Role", 
      values_to = "Driver"
    ) %>%
    mutate(Role = if_else(Role == "Driver", "Overtaker", "Passed")) %>%
    mutate(LapNumber_List = map(BaseLap, ~ seq(.x - 2, .x))) %>%
    unnest(LapNumber_List) %>%
    rename(LapNumber = LapNumber_List) %>%
    filter(LapNumber >= 1) %>%
    left_join(laps_clean, by = c("Driver", "LapNumber"))
  
  overtakes1 <- overtake_context %>%
    group_by(OvertakeID, LapNumber) %>%
    mutate(
      DeltaSessionTime = if_else(Role == "Overtaker", 
                                 LapStartTime - LapStartTime[Role == "Passed"], 
                                 LapStartTime - LapStartTime[Role == "Overtaker"]),
      DeltaLapTime = if_else(Role == "Overtaker", 
                             LapTimeCalculated - LapTimeCalculated[Role == "Passed"], 
                             LapTimeCalculated - LapTimeCalculated[Role == "Overtaker"]),
      DeltaS1Time  = if_else(Role == "Overtaker", 
                             Sector1 - Sector1[Role == "Passed"], 
                             Sector1 - Sector1[Role == "Overtaker"]),
      DeltaS2Time  = if_else(Role == "Overtaker", 
                             Sector2 - Sector2[Role == "Passed"], 
                             Sector2 - Sector2[Role == "Overtaker"]),
      DeltaS3Time  = if_else(Role == "Overtaker", 
                             Sector3 - Sector3[Role == "Passed"], 
                             Sector3 - Sector3[Role == "Overtaker"]),
      DeltaTyreLife = if_else(Role == "Overtaker", 
                              TyreLife - TyreLife[Role == "Passed"], 
                              TyreLife - TyreLife[Role == "Overtaker"]),
      DeltaSpeedI1 = if_else(Role == "Overtaker", 
                             SpeedI1 - SpeedI1[Role == "Passed"], 
                             SpeedI1 - SpeedI1[Role == "Overtaker"]),
      DeltaSpeedI2 = if_else(Role == "Overtaker", 
                             SpeedI2 - SpeedI2[Role == "Passed"], 
                             SpeedI2 - SpeedI2[Role == "Overtaker"]),
      DeltaSpeedFL = if_else(Role == "Overtaker", 
                             SpeedFL - SpeedFL[Role == "Passed"], 
                             SpeedFL - SpeedFL[Role == "Overtaker"]),
      DeltaSpeedST = if_else(Role == "Overtaker", 
                             SpeedST - SpeedST[Role == "Passed"], 
                             SpeedST - SpeedST[Role == "Overtaker"])
    ) %>%
    ungroup()
  
  rm(overtake_context)
  
  overtakes2 <- overtakes1 %>%
    mutate(CompoundFactor = case_when(
      Compound %in% c("SOFT", "ULTRASOFT", "SUPERSOFT", "HYPERSOFT") ~ 1L,
      Compound == "MEDIUM" ~ 2L,
      Compound == "HARD" ~ 3L,
      Compound %in% c("WET", "INTERMEDIATE") ~ 4L,
      TRUE ~ 0L
    ))
  
  rm(overtakes1)
  
  overtakes3 <- overtakes2 %>%
    group_by(OvertakeID, Driver) %>%
    #arrange(LapNumber) %>%
    mutate(Sector1SessionTime = if_else(is.na(Sector1SessionTime),
                                        LapStartTime + Sector1,
                                        Sector1SessionTime),
           Sector2SessionTime = if_else(is.na(Sector2SessionTime),
                                        Sector1SessionTime + Sector2,
                                        Sector2SessionTime),
           Sector3SessionTime = if_else(is.na(Sector3SessionTime),
                                        Sector2SessionTime + Sector3,
                                        Sector3SessionTime)) %>%
    ungroup()
  
  rm(overtakes2)
  
  overtakes4 <- overtakes3 %>%
    group_by(OvertakeID, LapNumber) %>%
    mutate(DeltaSector1SessionTime = if_else(Role == "Overtaker",
                                             Sector1SessionTime - Sector1SessionTime[Role == "Passed"],
                                             Sector1SessionTime - Sector1SessionTime[Role == "Overtaker"]),
           DeltaSector2SessionTime = if_else(Role == "Overtaker",
                                             Sector2SessionTime - Sector2SessionTime[Role == "Passed"],
                                             Sector2SessionTime - Sector2SessionTime[Role == "Overtaker"]),
           DeltaSector3SessionTime = if_else(Role == "Overtaker",
                                             Sector3SessionTime - Sector3SessionTime[Role == "Passed"],
                                             Sector3SessionTime - Sector3SessionTime[Role == "Overtaker"]),
           OTSector = case_when(
             DeltaSector1SessionTime[Role == "Overtaker"] < 0 ~ 1L,
             DeltaSector1SessionTime[Role == "Overtaker"] > 0 & 
               DeltaSector2SessionTime[Role == "Overtaker"] < 0 ~ 2L,
             DeltaSector1SessionTime[Role == "Overtaker"] > 0 & 
               DeltaSector2SessionTime[Role == "Overtaker"] > 0 & 
               DeltaSector3SessionTime[Role == "Overtaker"] < 0 ~ 3L,
             TRUE ~ 0L
           )
    ) %>%
    ungroup()
  
  rm(overtakes3)
  
  overtakes5 <- overtakes4 %>%
    group_by(OvertakeID, LapNumber) %>%
    mutate(
      OvertakeIntervall_Bottom = case_when(
        BaseLap != LapNumber ~ NA_real_,
        DeltaSector1SessionTime[Role == "Overtaker"] < 0 
        ~ LapStartTime[Role == "Overtaker"],
        DeltaSector1SessionTime[Role == "Overtaker"] > 0 & DeltaSector2SessionTime[Role == "Overtaker"] < 0 
        ~ Sector1SessionTime[Role == "Overtaker"],
        DeltaSector1SessionTime[Role == "Overtaker"] > 0 & DeltaSector2SessionTime[Role == "Overtaker"] > 0 & DeltaSector3SessionTime[Role == "Overtaker"] < 0 
        ~ Sector2SessionTime[Role == "Overtaker"],
        TRUE ~NA_real_
      ),
      OvertakeIntervall_Upper = case_when(
        BaseLap != LapNumber ~ NA_real_,
        DeltaSector1SessionTime[Role == "Overtaker"] < 0 
        ~ Sector1SessionTime[Role == "Overtaker"],
        DeltaSector1SessionTime[Role == "Overtaker"] > 0 & DeltaSector2SessionTime[Role == "Overtaker"] < 0 
        ~ Sector2SessionTime[Role == "Overtaker"],
        DeltaSector1SessionTime[Role == "Overtaker"] > 0 & DeltaSector2SessionTime[Role == "Overtaker"] > 0 & DeltaSector3SessionTime[Role == "Overtaker"] < 0 
        ~ Time[Role == "Overtaker"],
        TRUE ~ NA_real_
      )
    ) %>%
    ungroup()
  
  overtakes5$Sikeres <- 1L
  
  return(overtakes5)
}

#### rm az újboli futtatásokhoz ####

unique(all_laps$Season)
rm(all_laps, all_overtakes, season, otperround, otsector, deltas)

#### Adatok behívása, újboli gyors check ####

all_laps <- read_parquet("2018/laps_all_season/laps_2018.parquet")
season <- 2018L

all_laps <- read_parquet("2019/laps_all_season/laps_2019.parquet")
season <- 2019L

all_laps <- read_parquet("2020/laps_all_season/laps_2020.parquet")
season <- 2020L

all_laps <- read_parquet("2021/laps_all_season/laps_2021.parquet")
season <- 2021L

all_laps <- read_parquet("2022/laps_all_season/laps_2022.parquet")
season <- 2022L

all_laps <- read_parquet("2023/laps_all_season/laps_2023.parquet")
season <- 2023L

all_laps <- read_parquet("2024/laps_all_season/laps_2024.parquet")
season <- 2024L

all_laps <- read_parquet("2025/laps_all_season/laps_2025.parquet")
season <- 2025L

#### Futtatás az összes futamra ####

all_overtakes <- all_laps %>%
  group_split(Round) %>%
  map_dfr(elozes_fgny)

#### Ellenőrzés ####

unique(all_overtakes$Sikeres)
unique(all_overtakes$Season)

#előzések száma

otperround <- all_overtakes %>%
  group_by(Round, Location) %>%
  summarise(n_distinct_overtakes = n_distinct(OvertakeID), .groups = "drop")

sum(otperround$n_distinct_overtakes)

#otsector <- all_overtakes %>%
#  select(OvertakeID, Driver, Role, BaseLap, LapNumber, OTSector)

#deltak ellenőrzése

#sapply(all_overtakes[, c("DeltaSessionTime", "DeltaSector1SessionTime", "DeltaSector2SessionTime", "DeltaSector3SessionTime")], summary)

deltas <- all_overtakes %>%
  select(OvertakeID, Driver, Role, BaseLap, LapNumber, Time, LapTime, DeltaSessionTime,
         DeltaS1Time, DeltaS2Time, DeltaS3Time, DeltaSector1SessionTime, 
         DeltaSector2SessionTime, DeltaSector3SessionTime)

#### Mentés ####

#mappa <- "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/adatok/all_overtakes_wodrs"

write_parquet(all_overtakes, sink = file.path(mappa, paste0(season, "_success.parquet")))
