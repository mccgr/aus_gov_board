library(RPostgreSQL)
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(PostgreSQL())
directors <- tbl(pg, sql("SELECT * FROM aus_gov_board.directors"))

library(googlesheets)
# Use gs_auth() if necessary
gs <- gs_key("1_qnANgOSu3KUJTAkfzCwHRaC9t2PnmVXAvTkJIB3uII")

titles_df <- gs_read(gs, ws = "titles")
first_names_df <- gs_read(gs, ws = "first_names")
names_df <- gs_read(gs, ws = "names")

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
    collect() %>%
    mutate(suffixes = str_extract_all(last_name, "(?<=\\s)[A-Z,\\.]{2,}(?: \\(?Ret'?d\\)?)?")) %>%
    rowwise() %>%
    mutate(suffixes = paste_suffixes(suffixes)) %>%
    mutate(suffix_regex = str_c(str_replace_all(suffixes, "(\\(|\\))", "\\\\\\1"), "$")) %>%
    mutate(last_name = str_trim(last_name)) %>%
    mutate(last_name = if_else(!is.na(suffixes), 
                               str_trim(str_replace(last_name, suffix_regex, "")),
                               last_name)) %>%
    select(-fix_name, -suffix_regex)
    
dir_titles

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
    
paste_suffixes <- function(x) {
    if(length(x) > 0) {
        return(str_flatten(x, " "))
    } else {
        return(NA)
    }
} 

dir_gender <-
    dir_titles %>% 
    left_join(
        names_df %>%
            select(appointee, gender, first_name, last_name, suffixes, title) %>%
            rename(manual_title = title, 
                   manual_gender = gender,
                   manual_first_name = first_name,
                   manual_last_name = last_name,
                   manual_suffixes = suffixes)) %>%
    left_join(titles_df) %>%
    left_join(gendered_names) %>%
    mutate(gender = coalesce(manual_gender, gender, name_gender),
           title = coalesce(manual_title, title),
           last_name = coalesce(manual_last_name, last_name),
           first_name = coalesce(manual_first_name, first_name),
           suffixes = coalesce(manual_suffixes, suffixes)) %>%
    select(-gender_indicated, -name_gender, -matches("manual")) %>%
    ungroup() %>%
    mutate_all(function(x) if_else(x == "N/A", NA_character_, x))

dir_gender %>% count(gender)

rs <- dbExecute(pg, "SET search_path TO aus_gov_board")
dbWriteTable(pg, "gender", dir_gender, overwrite = TRUE, row.names = FALSE)

db_comment <- paste0(" 'CREATED USING create_gender.R ON ", Sys.time(), "'")

# Identify owners of the data
dbGetQuery(pg, "GRANT SELECT ON gender TO aus_gov_board_access")
dbGetQuery(pg, paste0("COMMENT ON TABLE gender IS ", db_comment))
dbGetQuery(pg, "ALTER TABLE gender OWNER TO aus_gov_board")

rs <- dbDisconnect(pg)
