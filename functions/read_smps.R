lin_mod_diameter = function(n, d, t){
  
  data.frame(
    n_start = n,
    d_start = d
  ) |> 
    tibble::tibble() |> 
    dplyr::mutate(
      d_end = dplyr::lead(d_start),
      n_end = dplyr::lead(n_start),
      idx = dplyr::row_number()
    ) |> 
    dplyr::nest_by(idx) |> 
    dplyr::mutate(
      data = data |> 
        tidyr::pivot_longer(tidyselect::everything(), names_sep = "_", names_to = c("type", "pos")) |> 
        tidyr::pivot_wider(names_from = "type") |> 
        list()
    ) |> 
    dplyr::mutate(
      mod = lm(formula = n~d, data = data) |> 
        list(),
      slope = coef(mod)[2],
      intercept = coef(mod)[1],
      d_start = data$d[1], 
      d_end = data$d[2],
      t = t
    ) |>
    dplyr::ungroup() |> 
    dplyr::select(t, d_start, d_end, slope, intercept)
}

read_smps = function(filepath, d_out = NULL){
  
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
  
  d_mod_list = list()
  
  # Do this without looping to speed up?
  cli::cli_progress_bar(
    format = "{cli::pb_spin} Regridding timestep: [{cli::pb_current}/{cli::pb_total}]. ETA:{cli::pb_eta}",
    total = nrow(dat_t))
  for(i in 1:nrow(dat_t)){
    cli::cli_progress_update()
    d_mod_list[[i]] = lin_mod_diameter(
      n = dat_n[i,], 
      d = dat_d[i,], 
      t = dat_t[i,1])
  }
  
  d_mod = dplyr::bind_rows(d_mod_list)
  
  if(is.null(d_out)){
    selected_d = tibble::tibble(d = seq(min(dat_d[1,], na.rm = T), max(dat_d[1,], na.rm = T), 30))
  }else{
    selected_d = tibble::tibble(d = d_out)
  }
  
  grid = data.frame(t = dat_t[,1]) |> 
    tibble::tibble(d = selected_d |> 
             list()) |> 
    tidyr::unnest(d)
  
  regridded_data = grid |> 
    dplyr::left_join(
      d_mod, 
      by = dplyr::join_by(t, dplyr::between(d, d_start, d_end, bounds = "[)"))
    ) |> 
    dplyr::mutate(n = (slope*d)+intercept,
                  dnd_logd = n/log(d))
  
  regridded_data
  
}

regridded_data = read_smps(
  filepath = "data/particles_non_ceda/raw/SMPS/C412 export.csv"
)

regridded_data_fine = read_smps(
  filepath = "data/particles_non_ceda/raw/SMPS/C412 export.csv",
  d_out = seq(20, 320, 1)
)

library(ggplot2)
library(patchwork)

g1 = regridded_data |> 
  dplyr::filter(t < 45000) |> 
  ggplot()+
  geom_tile(aes(t, d, fill = dnd_logd))+
  scale_fill_viridis_c(
    trans = scales::pseudo_log_trans(base = 10),
    breaks = c(-10^(c(1:3)),0,10^(c(1:5))),
    name = "d N/d ln(D)"
  )+
  theme_minimal()+
  ggtitle("30 nm")

g2 = regridded_data_fine |> 
  dplyr::filter(t < 45000) |> 
  ggplot()+
  geom_tile(aes(t, d, fill = dnd_logd))+
  scale_fill_viridis_c(
    trans = scales::pseudo_log_trans(base = 10),
    breaks = c(-10^(c(1:3)),0,10^(c(1:5))),
    name = "d N/d ln(D)"
  )+
  theme_minimal()+
  ggtitle("1 nm")



g1/g2


