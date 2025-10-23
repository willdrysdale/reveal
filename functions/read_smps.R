regrid_size_distribution = function(n, d, binOut){

  dat = data.frame(
    n = n,
    log10d_start = log10(d)
    ) |> 
    tibble::tibble() |> 
    dplyr::mutate(
      log10d_end = dplyr::lead(log10d_start),
      log10d_center = ((log10d_end-log10d_start)/2)+log10d_start
    )

  distribution <- approxfun(dat$log10d_center, dat$n, rule = 2)

  binOut |> 
    dplyr::rowwise() |> 
    dplyr::mutate(int = list(integrate(distribution, diameter_start, diameter_end))) |>
    dplyr::mutate(
      range = (diameter_end-diameter_start),
      n_out = int$value/range,
      n_out_abserr = int$abs.error) |>
    dplyr::select(-int)
  
}

read_smps_arrays = function(filepath){
  
  lines = readLines(filepath)
  
  length_of_lines = nchar(lines)
  
  zero_length_lines = which(length_of_lines == 0)
  
  start_d = 2
  
  start_n = zero_length_lines[1]+2
  
  start_t = zero_length_lines[2]+2
  
  dat_n = lines[start_n:(zero_length_lines[2]-1)] |> 
    stringr::str_split(",") |> 
    lapply(as.numeric) |> 
    do.call(rbind, args = _)
  
  na_rows = which(rowSums(is.na(dat_n)) != 0)
  
  dat_n = dat_n[-na_rows,]
  
  dat_d = lines[start_d:(zero_length_lines[1]-1)] |> 
    stringr::str_split(",") |> 
    lapply(as.numeric) |> 
    do.call(rbind, args = _) 
  
  dat_d = dat_d[-na_rows,]
  
  dat_t = lines[start_t:length(lines)] |> 
    stringr::str_split(",") |> 
    lapply(as.numeric) |> 
    do.call(rbind, args = _)
  
  dat_t = dat_t[-na_rows,]
  
  df_t = dat_t |> 
    as.data.frame() |> 
    tibble::tibble() |> 
    setNames(c("t", "tn"))
  
  list(number = dat_n,
       diamater = dat_d,
       time_total = df_t)
  
}

create_smps_binOut_from_lower_bounds = function(bin_lower, applyLog10 = TRUE){
  
  if(applyLog10){
    bin_lower = log10(bin_lower)
  }
  
  data.frame(
    diameter_start = bin_lower
  ) |> 
    dplyr::mutate(diameter_end = dplyr::lead(diameter_start)) |> 
    dplyr::filter(!is.na(diameter_end))
}

read_smps = function(filepath, binOut = NULL){
  
  smpsList = read_smps_arrays(filepath)
  
  if(is.null(binOut)){
    binOut = create_smps_binOut_from_lower_bounds(
      smpsList$d[1,]
    )
  }
  
  
  templist = list()
  for(i in 1:nrow(smpsList$n)){
    
    templist[[i]] = regrid_size_distribution(smpsList$n[i,], smpsList$d[i, ],binOut)
    
  }
  
  purrr::map_df(1:nrow(smpsList$n),
             ~{
               regrid_size_distribution(smpsList$n[.x,], smpsList$d[.x, ],binOut) |> 
                 dplyr::mutate(seconds_since_midnight = smpsList$time_total$t[.x])
               
             }, 
             binOut = binOut
             
             ) |> 
    dplyr::ungroup()

}


