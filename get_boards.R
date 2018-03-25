library(rvest)
library(dplyr, warn.conflicts = FALSE)
url <- "https://www.directory.gov.au/boards-and-other-entities"
html <- read_html(url) 

name <-
    html %>%
    html_nodes(".views-table") %>%
    .[[1]] %>%
    html_table() 

url <-
    html %>%
    html_nodes(".views-field") %>%
    html_children() %>% 
    html_attr('href')

library(rvest)
library(lubridate)
library(dplyr, warn.conflicts = FALSE)

get_board <- function(url) {
    full_url <- paste0("https://www.directory.gov.au/", url)
    table <-
        read_html(full_url) %>%
        html_nodes("table") 
    if (length(table) > 0) {
        res <- 
            table %>%
            .[[1]] %>%
            html_table() %>% 
            as_tibble() %>%
            mutate(url = url)
        return(res)
    }
}

links <- tibble(title = name$Title, url)

temp <- lapply(links$url, get_board)

fix_names <- function(df) {
    names(df) <- gsub("\\s+", "_", names(df))
    names(df) <- tolower(names(df))
    df
}

directors <- bind_rows(temp) %>% fix_names()

# Convert all dates in start_date and end_date with the empty string "" to NA, and set all dates with the entry 
# "Determined by the appointer" to NA as well

directors$start_date[nchar(directors$start_date) == 0] <- NA
directors$end_date[nchar(directors$end_date) == 0] <- NA
directors$end_date[directors$end_date == "Determined by the appointer" & !is.na(directors$end_date)] <- NA

# All other entries have valid dates. Use the dmy function from lubridate to convert to datetime format

directors$start_date <- dmy(directors$start_date)
directors$end_date <- dmy(directors$end_date)


library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

rs <- dbGetQuery(pg, "SET search_path TO aus_gov_board")
dbWriteTable(pg, "links", links, overwrite = TRUE, row.names = FALSE)
dbWriteTable(pg, "directors", directors, overwrite = TRUE, row.names = FALSE)

db_comment <- paste0(" 'CREATED USING get_boards.R ON ", Sys.time(), "'")

# Identify owners of the data
dbGetQuery(pg, "GRANT SELECT ON links TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE links IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE links OWNER TO aus_gov_board_access")

dbGetQuery(pg, "GRANT SELECT ON directors TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE directors IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE directors OWNER TO aus_gov_board_access")
dbDisconnect(pg)
