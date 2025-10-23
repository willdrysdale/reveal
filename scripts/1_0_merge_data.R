library(faamr)
library(dplyr)
library(purrr)
library(tidyr)
library(mirai)

source(here::here('functions','flight_details.R'))

flightFiles = flight_files()

daemons(8)

# Core --------------------------------------------------------------------

coreDat = mirai({
  flightFiles |> 
    dplyr::filter(fileType == "core.nc") |> 
    dplyr::rowwise() |> 
    dplyr::mutate(
      data = faamr::read_faam_core(
        filePath, 
        startDate = startDate, 
        endDate = endDate,
        selectVar = c("LAT_GIN", "LON_GIN", "ALT_GIN"),
        sps = 32,
        averageNanoString = "00:00:10"
      ) |> 
        list()) |> 
    dplyr::select(flightNumber, data) |> 
    tidyr::unnest(data) |> 
    tidyr::pivot_wider() |> 
    dplyr::select(-seconds_since_midnight)
},
flightFiles = flightFiles)


# Nitrate -----------------------------------------------------------------

nitrateDat = mirai({
  flightFiles |> 
    dplyr::filter(fileType == "core-nitrates.nc") |> 
    dplyr::rowwise() |> 
    dplyr::mutate(
      data = faamr::read_faam_nitrates(
        filePath, 
        startDate = startDate, 
        endDate = endDate,
        averageNanoString = "00:00:00.1",
        allowReducedQuality = TRUE,
        allowSuspect = TRUE
      ) |> 
        list()
    ) |> 
    dplyr::select(flightNumber, data) |> 
    tidyr::unnest(data) |>
    tidyr::separate_wider_delim(name, names = c("spec", "type"), delim =  "_") |> 
    tidyr::pivot_wider(names_from = "type") |> 
    dplyr::select(-flag) |> 
    dplyr::mutate(mr = ifelse(dplyr::between(mr, -1e6, 1e6), mr, NA)) |> 
    dplyr::mutate(
      date = nanotime::nano_floor(date, nanotime::as.nanoduration("00:00:10")),
      w = 1/(u^2),
      wx = mr*w
    ) |> 
    dplyr::group_by(flightNumber, date, spec) |> 
    dplyr::summarise(
      # mrw = sum(wx, na.rm = F)/sum(w, na.rm = T), # mixing ratio weighted by uncertainty
      # u_mrw = sqrt(1/sum(w, na.rm = T)), # combined uncertainty relating to mrw
      mr = mean(mr, na.rm = T), # mean
      u = sqrt((sum((u^2))/dplyr::n())), # root of sum of squares / N
      lod = mean(lod, na.rm = T)/sqrt(dplyr::n()), # mean LOD additionally reduced by ~10, as LOD is expected to improve by root of N samples
    ) |> 
    dplyr::ungroup() |> 
    tidyr::pivot_wider(
      names_from = "spec",
      values_from = c("mr", "u", "lod"), #c("mrw", "u_mrw", "mr", "u_mr", "lod"),
      names_glue = "{spec}_{.value}")
},
flightFiles = flightFiles)


# FGGA --------------------------------------------------------------------
# Currently not considering averaging on fgga uncer / bias
fggaDat = mirai({
  flightFiles |> 
    dplyr::rowwise() |> 
    dplyr::filter(fileType == "faam-fgga.na") |> 
    dplyr::mutate(
      data = faamr::read_faam_fgga(
        filePath, 
        averageNanoString = "00:00:10",
        extractUncert = T,
        applyBias = T
      ) |> 
        list()
    ) |> 
    dplyr::select(flightNumber, data) |> 
    tidyr::unnest(data)
},
flightFiles = flightFiles
)



# AMS ---------------------------------------------------------------------
amsDat = readRDS(here::here('data','particles_non_ceda','tidy','AMS.RDS'))


# SP2 ---------------------------------------------------------------------

sp2Dat = readRDS(here::here('data','particles_non_ceda','tidy','SP2.RDS'))


# SMPS --------------------------------------------------------------------

# TBC


# Resolve Async -----------------------------------------------------------

waiting = TRUE
cli::cli_progress_bar(format = "{cli::pb_spin} Waiting for Data Read. Elapses: {cli::pb_elapsed}")
while(waiting){
  cli::cli_progress_update()
  Sys.sleep(1)
  
  statusResolved = sum(c(unresolved(coreDat),unresolved(nitrateDat),unresolved(fggaDat)))
  
  if(statusResolved == 0){
    
    statusError = sum(c(is_error_value(coreDat[]),is_error_value(nitrateDat[]),is_error_value(fggaDat[])))
    
    if(statusError == 0){
      
      waiting = FALSE
      print("Data Read Complete")
      
    }else{
      
      waiting = FALSE
      print("Data Read Failed")
      
    }
    
  }
  
}


# Merge -------------------------------------------------------------------

dat = list(coreDat,
           nitrateDat,
           fggaDat,
           amsDat,
           sp2Dat
) |> 
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
