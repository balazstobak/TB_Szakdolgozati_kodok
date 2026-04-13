rm(list=ls())

library(dplyr)
library(tidyr)
library(purrr)
library(arrow)
library(lubridate)
library(stringr)
library(zoo)

#### Függvény ####

sikertelen_fgny <- function(laps) {
  
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
                                    LapTimeCalculated - Sector1Time - Sector2Time, Sector3Time),
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
        coalesce(Sector3_tmp, median(Sector3_tmp, na.rm = TRUE))
    ) %>%
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
      Pit == 0 & grepl("1", TrackStatus) & !is.na(LapTimeCalculated), 1L, 0L))) %>%
    ungroup()
  
  rm(laps6)
  
  laps8 <- laps7 %>%
    mutate(CompoundFactor = case_when(
      Compound %in% c("SOFT", "ULTRASOFT", "SUPERSOFT", "HYPERSOFT") ~ 1L,
      Compound == "MEDIUM" ~ 2L,
      Compound == "HARD" ~ 3L,
      Compound %in% c("WET", "INTERMEDIATE") ~ 4L,
      TRUE ~ 0L
    ))
  
  rm(laps7)
  
  driver_pairs <- laps8 %>%
    inner_join(laps8, 
               by = "LapNumber", 
               suffix = c("_Ahead", "_Behind"),
               relationship = "many-to-many") %>%
    filter(Position_Behind - Position_Ahead == 1) %>%
    filter(OTable_Ahead == 1L, Flag_Ahead == 0L, TrackStatus_Ahead == "1") %>%
    select(LapNumber, 
           Driver_Ahead, Position_Ahead, 
           Driver_Behind, Position_Behind)
  
  failed_attempts_streaks <- driver_pairs %>%
    arrange(Driver_Ahead, Driver_Behind, LapNumber) %>%
    group_by(Driver_Ahead, Driver_Behind) %>%
    mutate(
      Prev_Lap1 = lag(LapNumber, n = 1),
      Prev_Lap2 = lag(LapNumber, n = 2),
      Is_3_Consecutive = (LapNumber == Prev_Lap1 + 1) & (Prev_Lap1 == Prev_Lap2 + 1)
    ) %>%
    filter(Is_3_Consecutive == TRUE) %>%
    ungroup()
  
  failed_overtake_context <- failed_attempts_streaks %>%
    mutate(FailedID = row_number()) %>%
    select(FailedID, Driver_Ahead, Driver_Behind, BaseLap = LapNumber) %>%
    pivot_longer(
      cols = c(Driver_Ahead, Driver_Behind),
      names_to = "Role",
      values_to = "Driver"
    ) %>%
    mutate(Role = if_else(Role == "Driver_Ahead", "Passed", "Overtaker")) %>%
    mutate(LapNumber_List = map(BaseLap, ~ seq(.x - 2, .x))) %>%
    unnest(LapNumber_List) %>%
    rename(LapNumber = LapNumber_List) %>%
    arrange(FailedID, Role, LapNumber) %>% 
    left_join(laps8, by = c("Driver", "LapNumber"))
  
  foc_filt <- failed_overtake_context %>%
    group_by(FailedID, LapNumber) %>%
    mutate(
      DeltaSessionTime = if_else(Role == "Overtaker", 
                                 LapStartTime - LapStartTime[Role == "Passed"], 
                                 LapStartTime - LapStartTime[Role == "Overtaker"])
    ) %>%
    ungroup() %>% 
    group_by(FailedID) %>%
    filter(abs(DeltaSessionTime) < 1.5, na.rm = TRUE) %>% 
    filter(n() == 6) %>% 
    ungroup()
  
  ###
  
  valid_ids <- foc_filt %>%
    group_by(FailedID) %>%
    filter(
      n_distinct(Driver[Role == "Overtaker"]) == 1,
      n_distinct(Driver[Role == "Passed"]) == 1
    ) %>%
    pull(FailedID) %>%
    unique()
  
  battle_summaries <- foc_filt %>%
    filter(FailedID %in% valid_ids) %>%
    group_by(FailedID, BaseLap) %>%
    summarise(
      Overtaker      = unique(Driver[Role == "Overtaker"]),
      Passed         = unique(Driver[Role == "Passed"]),
      BaseLap_Delta  = abs(DeltaSessionTime[LapNumber == BaseLap & Role == "Overtaker"]),
      .groups        = "drop"
    )
  
  ###
  
  best_failed_attempts <- battle_summaries %>%
    arrange(Overtaker, Passed, BaseLap) %>%
    group_by(Overtaker, Passed) %>%
    mutate(
      Lap_Gap = BaseLap - lag(BaseLap, default = first(BaseLap)),
      Is_New_Streak = as.integer(Lap_Gap > 3), 
      Streak_ID = cumsum(Is_New_Streak)
    ) %>%
    group_by(Overtaker, Passed, Streak_ID) %>%
    # Itt most már az Avg_Delta helyett a BaseLap_Delta alapján keressük a legkisebbet:
    slice_min(order_by = BaseLap_Delta, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  # 3. Az eredeti (részletes) tábla visszaszűrése csak a kiválasztott ID-kra
  final_foc <- foc_filt %>%
    filter(FailedID %in% best_failed_attempts$FailedID)
  
  rm(battle_summaries, best_failed_attempts, foc_filt)

  final_foc <- final_foc %>%
    group_by(FailedID, LapNumber) %>%
    mutate(
      Sector1SessionTime = if_else(is.na(Sector1SessionTime),
                                   LapStartTime + Sector1,
                                   Sector1SessionTime),
      Sector2SessionTime = if_else(is.na(Sector2SessionTime),
                                   Sector1SessionTime + Sector2,
                                   Sector2SessionTime),
      Sector3SessionTime = if_else(is.na(Sector3SessionTime),
                                   Sector2SessionTime + Sector3,
                                   Sector3SessionTime),
      DeltaSector1SessionTime = if_else(Role == "Overtaker",
                                        Sector1SessionTime - Sector1SessionTime[Role == "Passed"],
                                        Sector1SessionTime - Sector1SessionTime[Role == "Overtaker"]),
      DeltaSector2SessionTime = if_else(Role == "Overtaker",
                                        Sector2SessionTime - Sector2SessionTime[Role == "Passed"],
                                        Sector2SessionTime - Sector2SessionTime[Role == "Overtaker"]),
      DeltaSector3SessionTime = if_else(Role == "Overtaker",
                                        Sector3SessionTime - Sector3SessionTime[Role == "Passed"],
                                        Sector3SessionTime - Sector3SessionTime[Role == "Overtaker"]),
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
  
  final_foc$Sikeres <- 0L
  
  return(final_foc)
}

#### rm az újboli futtatásokhoz ####

unique(all_laps$Season)
rm(all_laps, all_sikertelen, season, na_counts, duplic, failedperround, deltas)

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

all_sikertelen <- all_laps %>%
  group_split(Round, Location) %>%
  map_dfr(sikertelen_fgny)

#### Ellenőrzés ####

unique(all_sikertelen$Sikeres)
unique(all_sikertelen$Season)

#sikertelenek száma

failedperround <- all_sikertelen %>%
  group_by(Round, Location) %>%
  summarise(n_distinct_failed = n_distinct(FailedID), .groups = "drop")

sum(failedperround$n_distinct_failed)

#### Mentés ####

#mappa <- "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/adatok/all_overtakes_wodrs/sikertelen"

write_parquet(all_sikertelen, sink = file.path(mappa, paste0(season, "_failed.parquet")))

total_unique_overtakes <- all_sikertelen %>%
  distinct(Round, Location, FailedID) %>%
  nrow()

print(total_unique_overtakes)

