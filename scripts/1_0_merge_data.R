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
  mutate(mr = ifelse(between(mr, -1e6, 1e6), mr, NA)) |> 
  mutate(
    date = nanotime::nano_floor(date, nanotime::as.nanoduration("00:00:10")),
    w = 1/(u^2),
    wx = mr*w
  ) |> 
  group_by(flightNumber, date, spec) |> 
  summarise(
    # mrw = sum(wx, na.rm = F)/sum(w, na.rm = T), # mixing ratio weighted by uncertainty
    # u_mrw = sqrt(1/sum(w, na.rm = T)), # combined uncertainty relating to mrw
    mr = mean(mr, na.rm = T), # mean
    u = sqrt((sum((u^2))/n())), # root of sum of squares / N
    lod = mean(lod, na.rm = T)/sqrt(n()), # mean LOD additionally reduced by ~10, as LOD is expected to improve by root of N samples
  ) |> 
  ungroup() |> 
  pivot_wider(
    names_from = "spec",
    values_from = c("mr", "u", "lod"), #c("mrw", "u_mrw", "mr", "u_mr", "lod"),
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
write.csv(dat, here::here('data','faam_merge','reveal_merge.csv'))

# SWAS --------------------------------------------------------------------

swasFiles = list.files(here::here('data','was'), full.names = T)

datSwas = map_df(swasFiles, read_york_gc_fid_lab) |> 
  nest_join(dat, 
            by = join_by(between(y$date, x$start_sample_date_time, x$stop_sample_date_time))) |> 
  rowwise() |> 
  mutate(dat = dat |> 
           select(-flightNumber) |> 
           summarise_all(mean, na.rm = T),
         flight = tolower(flight)
           ) |> 
  rename(flightNumber = flight) |> 
  unnest(dat)

saveRDS(datSwas, here::here('data','faam_merge','swasMerge.RDS'))
write.csv(datSwas, here::here('data','faam_merge','swas_merge.csv'))
