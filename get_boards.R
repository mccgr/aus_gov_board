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

directors <- 
    bind_rows(temp) %>% 
    fix_names() %>%
    mutate(end_date = if_else(end_date == "Determined by the appointer", NA_character_, end_date)) %>%
    mutate(start_date = if_else(start_date == "", NA_character_, start_date),
           end_date = if_else(end_date == "", NA_character_, end_date)) %>%
    mutate_at(c("start_date", "end_date"), dmy)

library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

rs <- dbExecute(pg, "SET search_path TO aus_gov_board")
dbWriteTable(pg, "links", links, overwrite = TRUE, row.names = FALSE)
dbWriteTable(pg, "directors", directors, overwrite = TRUE, row.names = FALSE)

db_comment <- paste0(" 'CREATED USING get_boards.R ON ", Sys.time(), "'")

# Identify owners of the data
dbGetQuery(pg, "GRANT SELECT ON links TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE links IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE links OWNER TO aus_gov_board")

dbGetQuery(pg, "GRANT SELECT ON directors TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE directors IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE directors OWNER TO aus_gov_board")
dbDisconnect(pg)
