library(dplyr, warn.conflicts = FALSE)
library(readr)
library(lubridate)
fix_names <- function(df) {
    names(df) <- gsub("\\s+", "_", names(df))
    names(df) <- gsub("[\\$\\(\\)]", "", names(df))
    names(df) <- gsub("_$", "", names(df))
    names(df) <- tolower(names(df))
    df
}

df <- 
    read_csv("data/AusGovBoards Appointments 2017-06-30 .csv",
                    na = c("N/A", ".", ""), locale=locale(date_format = "%d/%M/%Y"),
                    guess_max = 20000) %>%
    fix_names() %>%
    mutate(initial_start_date = dmy(initial_start_date))
     
df %>% select(1:5+20)

library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

rs <- dbExecute(pg, "SET search_path TO aus_gov_board")
dbWriteTable(pg, "appointments", df, overwrite = TRUE, row.names = FALSE)

db_comment <- paste0(" 'CREATED USING import_appointments.R ON ", Sys.time(), "'")

# Identify owners of the data
dbGetQuery(pg, "GRANT SELECT ON appointments TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE appointments IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE appointments OWNER TO aus_gov_board")

dbGetQuery(pg, "GRANT SELECT ON directors TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE directors IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE directors OWNER TO aus_gov_board")
dbDisconnect(pg)

