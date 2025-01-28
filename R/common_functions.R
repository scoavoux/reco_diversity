recode_vars <- function(char, .scheme){
  e <- readr::read_csv(here::here("data", "codes.csv"), col_types = "ccc")
  e <- dplyr::filter(e, scheme == .scheme)
  val <- e$replacement
  names(val) <- e$orig
  return(val[char])
}