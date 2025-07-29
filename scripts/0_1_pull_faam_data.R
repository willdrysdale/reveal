library(faamr)
library(keyring)

source(here::here('functions','flight_details.R'))

flightDetails = flight_details()

dirOut = here::here('data', 'faam_raw')

if(!dir.exists(dirOut)){
  dir.create(dirOut)
}


flight_download(
  flightDetails$flightNumber,
  user = key_get("ceda_user"),
  pass = key_get("ceda", key_get("ceda_user")),
  dirOut = dirOut,
  files = c("flight-sum.txt", "core.nc", "faam-fgga.na", "core-nitrates.nc")
)
