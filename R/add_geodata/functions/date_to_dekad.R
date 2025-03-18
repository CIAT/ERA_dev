#' Format date as yearly or monthly dekad (10-day period)
#'
#' `date_to_dekad` transforms a class `Date` object to dekad of the year or month.
#'
#' This function is taken from Melanie Bacou's https://github.com/tidyverse/lubridate/issues/617#issuecomment-521393447
#'
#' @param date class `Date` vector to convert to dekad
#' @param type dekad of `month` (1:3) or `year` (1:36)
#' @return integer dekad
#' @importFrom lubridate day
#' @examples
#' dekad(Sys.Date())
#' @export
date_to_dekad <- function(date,
                      type = c("month", "year"),
                      ...) {
  type <- match.arg(type)
  x <- as.Date(date, ...)
  res <- ifelse(day(x) > 20,  3, ifelse(lubridate::day(x) > 10, 2, 1))
  if(type == "year") res <- month(x)*3 + res - 3
  return(res)
}
