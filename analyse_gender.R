library(RPostgreSQL)
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(PostgreSQL())
directors <- tbl(pg, sql("SELECT * FROM aus_gov_board.directors"))

library(googlesheets)
# Use gs_auth() if necessary
gs <- gs_key("1_qnANgOSu3KUJTAkfzCwHRaC9t2PnmVXAvTkJIB3uII")

titles_df <- gs_read(gs, ws="titles")
first_names_df <- gs_read(gs, ws="first_names")
names_df <- gs_read(gs, ws="names")

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
            select(appointee, gender) %>%
            rename(manual_gender = gender)) %>%
    left_join(titles_df) %>%
    left_join(gendered_names) %>%
    mutate(gender = coalesce(manual_gender, gender, name_gender)) %>%
    select(-gender_indicated, -name_gender, -manual_gender)

dir_gender %>% count(gender)

rs <- dbDisconnect(pg)