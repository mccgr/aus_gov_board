library(RPostgreSQL)
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(PostgreSQL())
directors <- tbl(pg, sql("SELECT * FROM aus_gov_board.directors"))

library(googlesheets)
# Use gs_auth() if necessary
gs <- "1_qnANgOSu3KUJTAkfzCwHRaC9t2PnmVXAvTkJIB3uII"

titles_df <- sheets_read(gs, sheet="titles")
first_names_df <- sheets_read(gs, sheet="first_names")
names_df <- sheets_read(gs, sheet="names")

paste_suffixes <- function(x) {
    if(length(x) > 0) {
        return(str_flatten(x, " "))
    } else {
        return(NA)
    }
} 

make_regex <- function(titles) {
    
    clean_titles <- function(titles) {
        titles_clean <- gsub("\\(", "\\\\(", titles)
        titles_clean <- gsub("\\)", "\\\\)", titles_clean)
        titles_clean <- gsub("\\.", "\\\\.", titles_clean)
        titles_clean
    }
    
    titles_regex <- paste0("(?:", paste(clean_titles(titles),
                                    collapse = "|"), ")")
    paste0("^(", titles_regex, ")\\s*(.*?)$")
}

regex <- make_regex(titles_df$title)

dir_titles <-
    directors %>% 
    filter(appointee != "VACANT") %>%
    mutate(appointee_clean = replace(appointee, "'", "")) %>%
    mutate(appointee_clean = regexp_replace(appointee, "^Member\\s+", "")) %>%
    mutate(temp = regexp_matches(appointee_clean, regex)) %>%
    mutate(title = sql("temp[1]"),
           name = sql("temp[2]")) %>%
    mutate(fix_name = name %!~% '\\s+') %>%
    mutate(title = if_else(fix_name, NA, title),
           name = if_else(fix_name, appointee_clean, name)) %>%
    mutate(temp2 = regexp_matches(name, "^([^\\s]+)\\s+(.*)$")) %>%
    mutate(first_name = sql("temp2[1]"),
           last_name = sql("temp2[2]")) %>%
    select(appointee, first_name, last_name, title, fix_name) %>%
    collect()
dir_titles

dir_titles %>% 
    left_join(titles_df) %>% 
    count(gender)

# If names occurs at least 6 times and maps to only one gender
# consider name to be gender-specific.
gendered_names <-
    dir_titles %>% 
    inner_join(titles_df) %>%
    filter(!is.na(gender)) %>%
    select(first_name, gender) %>%
    group_by(first_name) %>%
    filter(n_distinct(gender) == 1, n() >= 6) %>%
    rename(name_gender = gender) %>%
    distinct() %>%
    bind_rows(first_names_df)
    
dir_gender <-
    dir_titles %>% 
    left_join(
        names_df %>%
            select(appointee, gender, first_name, last_name) %>%
            rename(manual_gender = gender,
                   manual_first_name = first_name,
                   manual_last_name = last_name)) %>%
    left_join(titles_df) %>%
    left_join(gendered_names) %>%
    mutate(gender = coalesce(manual_gender, gender, name_gender),
           last_name = coalesce(manual_last_name, last_name),
           first_name = coalesce(manual_first_name, first_name)) %>%
    select(-gender_indicated, -name_gender, -matches("manual")) 

dir_gender %>% count(gender)

rs <- dbExecute(pg, "SET search_path TO aus_gov_board")
dbWriteTable(pg, "gender", dir_gender, overwrite = TRUE, row.names = FALSE)

db_comment <- paste0(" 'CREATED USING create_gender.R ON ", Sys.time(), "'")

# Identify owners of the data
dbGetQuery(pg, "GRANT SELECT ON gender TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE gender IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE gender OWNER TO aus_gov_board")

rs <- dbDisconnect(pg)
