#' This function allows you to easily pull clean looker data from a look.
#' @param look_id Look ID to pull.
#' @param file_name Name for pull to avoid rerunning. Defaults to looker_pull.
#' @param re_run If TRUE, overwrites any existing data. Defauts to FALSE.
#' @keywords database
#' @export
#' @examples
#' looker_pull()

looker_pull <- function(look_id, 
                        file_name = 'looker_pull', 
                        re_run = FALSE, 
                        path = '~/wellbe-analytics/LookRApiCredentials.R') {
  
  source(path)
  
  pull_file_name <- paste0(file_name, '.Rda')
  
  re_run_checks <- c(!file.exists(pull_file_name), # are there no files currently saved?
                     re_run == TRUE,               # are we saying it should be rerun?
                     file_name == 'looker_pull')   # Did they make a unique file name?
  
  if (any(re_run_checks == TRUE)) {
    # devtools::install_github("looker/lookr", force = T)
    LookR::looker_setup(id = IdLookR,
                        secret = secretLookR,
                        api_path = api_pathLookR)
    
    looker_raw <- as.data.frame(run_look(look_id))
    # rm(list = c('IdLookR', 'secretLookR', 'api_pathLookR'))
    
    looker_raw[] <- lapply(looker_raw, function(x) type.convert(as.character(x), as.is = TRUE)) # autoconvert variables
    names(looker_raw) <- gsub("^.*\\.","", names(looker_raw)) # Remove prepending

    save(looker_raw, file = pull_file_name)
    
  } else {
    load(pull_file_name)
  }
  looker_raw
}

#' This function allows you to run SQL queries from the read replica of the Wellbe Database
#' @param SQL SQL query to run.
#' @param path Path to where DB credentials are. Defauts to where credentials are saved on Andrew's work computer.
#' @keywords database
#' @importFrom DBI dbConnect dbDriver
#' @import RPOSTgreSQL
#' @export
#' @examples
#' read_replica_query()

read_replica_query <- function(sql, path = "~/wellbe-analytics/dbicred.r") {
  # library(DBI)
  library(RPostgreSQL)
  source(path)
  
  drv <- DBI::dbDriver("PostgreSQL") 
  con <- DBI::dbConnect(drv,
                   user = usr, 
                   password = pw, 
                   dbname = dbn, 
                   host = hst, 
                   port = prt)
  on.exit(dbDisconnect(con))
  dbGetQuery(con, sql)
}