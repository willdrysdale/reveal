library(DBI)
library(faamr)
library(dplyr)
library(purrr)
library(tidyr)

averageString = "00:00:00.1"

flightFiles = list_flight_data_local(here::here('data','faam_raw')) |> 
  nest_by(flightNumber) |> 
  mutate(corePath = data |> 
           filter(fileType == "core.nc") |> 
           pull(filePath),
         wow = map2(corePath,flightNumber, 
                    \(x,y) get_weight_off_wheels(x) |> 
                      summarise(startDate = min(date),
                                endDate = max(date))
         )) |> 
  unnest(wow) |> 
  unnest(data) |> 
  select(-corePath)

coreDat = flightFiles |> 
  filter(fileType == "core.nc") |> 
  rowwise() |> 
  mutate(
    data = read_faam_core(
      filePath, 
      startDate = startDate, 
      endDate = endDate,
      selectVar = c("LAT_GIN", "LON_GIN", "ALT_GIN"),
      sps = 32,
      averageNanoString = averageString
    ) |> 
      list()) |> 
  select(flightNumber, data) |> 
  unnest(data) |> 
  pivot_wider() |> 
  select(-seconds_since_midnight)

nitrateDat = flightFiles |> 
  filter(fileType == "core-nitrates.nc") |> 
  mutate(
    data = read_faam_nitrates(
      filePath, 
      startDate = startDate, 
      endDate = endDate,
      averageNanoString = averageString
    ) |> 
      list()
  ) |> 
  select(flightNumber, data) |> 
  unnest(data) |> 
  filter(between(value, -1000, 1e5)) |> 
  pivot_wider() |> 
  select(-seconds_since_midnight)

fggaDat = flightFiles |> 
  filter(fileType == "faam-fgga.na") |> 
  mutate(
    data = read_faam_fgga(
      filePath, 
      averageNanoString = averageString
    ) |> 
      list()
  ) |> 
  select(flightNumber, data) |> 
  unnest(data)

dat = list(coreDat,
           nitrateDat,
           fggaDat) |> 
  reduce(left_join, by = c("flightNumber", "date")) |> 
  ungroup()

saveRDS(dat, here::here('data','faam_merge','merge.RDS'))
