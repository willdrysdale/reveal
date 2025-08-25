library(faamr)
library(dplyr)
library(purrr)
library(tidyr)

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

# Core --------------------------------------------------------------------

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
      averageNanoString = "00:00:10"
    ) |> 
      list()) |> 
  select(flightNumber, data) |> 
  unnest(data) |> 
  pivot_wider() |> 
  select(-seconds_since_midnight)


# Nitrate -----------------------------------------------------------------

nitrateDat = flightFiles |> 
  filter(fileType == "core-nitrates.nc") |> 
  mutate(
    data = read_faam_nitrates(
      filePath, 
      startDate = startDate, 
      endDate = endDate,
      averageNanoString = "00:00:00.1",
      allowReducedQuality = TRUE,
      allowSuspect = TRUE
    ) |> 
      list()
  ) |> 
  select(flightNumber, data) |> 
  unnest(data) |>
  separate_wider_delim(name, names = c("spec", "type"), delim =  "_") |> 
  pivot_wider(names_from = "type") |> 
  select(-flag) |> 
  mutate(
    date = nanotime::nano_floor(date, nanotime::as.nanoduration("00:00:10")),
    w = 1/(u^2),
    wx = mr*w
  ) |> 
  group_by(flightNumber, date, spec) |> 
  summarise(
    mrw = sum(wx, na.rm = F)/sum(w, na.rm = T), # mixing ratio weighted by uncertainty
    u_mrw = sqrt(1/sum(w, na.rm = T)), # combined uncertainty relating to mrw
    mr = mean(mr, na.rm = T), # mean
    u_mr = sqrt(sum((u^2))), # root of sum of squares
    lod = mean(lod, na.rm = T)/10, # mean LOD additionally reduced by 10, as LOD is expeceted to improve by root of change in orders of magnitude 0.1s -> 10s == x 100, therfore / sqrt(100) == 10 
  ) |> 
  ungroup() |> 
  pivot_wider(
    names_from = "spec",
    values_from = c("mrw", "u_mrw", "mr", "u_mr", "lod"),
    names_glue = "{spec}_{.value}")


# FGGA --------------------------------------------------------------------
# Currently not considering averaging on fgga uncer / bias
fggaDat = flightFiles |> 
  filter(fileType == "faam-fgga.na") |> 
  mutate(
    data = read_faam_fgga(
      filePath, 
      averageNanoString = "00:00:10",
      extractUncert = T,
      applyBias = T
    ) |> 
      list()
  ) |> 
  select(flightNumber, data) |> 
  unnest(data)


# Merge -------------------------------------------------------------------

dat = list(coreDat,
           nitrateDat,
           fggaDat) |> 
  reduce(left_join, by = c("flightNumber", "date")) |> 
  ungroup()

saveRDS(dat, here::here('data','faam_merge','merge.RDS'))
