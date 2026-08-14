#!/usr/bin/env Rscript
# Merge this dataset with ICPSR 226001 (Di Tella, Kotti, Le Pennec & Pons).
#
# The two datasets do not overlap in years, so the merge is a vertical stack,
# not a join. What makes it non-trivial is that the two sides use different
# column names, different keys, and that 2018 appears in both.
#
# Usage:
#   Rscript scripts/merge_with_icpsr.R --icpsr <dir> --ours <panel_icpsr_compat.csv> \
#     [--out merged.csv] [--test]
#
#   <dir> is the ICPSR 226001 folder holding candidates_complexity.csv and
#   candidates_topics.csv.
#
# Or source it and call merge_icpsr() yourself.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(stringr)
})

# The 31 topic categories, in the order both datasets use.
TOPICS <- c(
  "Agriculture and Farmers", "Centralisation", "Civic Mindedness",
  "Constitutionalism", "Culture", "Democracy", "Economic Planning",
  "Education", "Equality", "Foreign Special Relationships",
  "Free Market Economy", "Freedom and Human Rights",
  "Governmental and Administrative Efficiency", "Incentives",
  "Internationalism", "Labour Groups", "Law and Order", "Market Regulation",
  "Military", "Multiculturalism", "National Way of Life", "Other", "Peace",
  "Political Authority", "Political Corruption", "Protectionism",
  "Social Groups", "Sustainability", "Technology and Infrastructure",
  "Traditional Morality", "Welfare State")

#' Stack ICPSR 226001 and this dataset into one candidate-year panel.
#'
#' @param icpsr_dir folder with candidates_complexity.csv and candidates_topics.csv
#' @param ours_path path to panel_icpsr_compat.csv
#' @param drop_icpsr_2018 belt and braces. ICPSR's 2018 stage-2 rows are all
#'   sourced from their PRIMARY scrape (0 of 76 come from general_wayback), so
#'   the data_source filter above already removes them. Those captures cluster
#'   months before the general election, which is why we ship our own 2018.
#' @return a tibble with one row per candidate-year and a `source` column.
merge_icpsr <- function(icpsr_dir, ours_path, drop_icpsr_2018 = TRUE) {

  # --- their side -------------------------------------------------------
  # Restrict to what is comparable with ours: the general election (stage 2)
  # scraped from the Wayback Machine. Their `stage == 1` rows are primaries,
  # which this dataset does not cover.
  cx <- read_csv(file.path(icpsr_dir, "candidates_complexity.csv"),
                 show_col_types = FALSE) %>%
    filter(stage == 2, data_source == "general_wayback")

  tp <- read_csv(file.path(icpsr_dir, "candidates_topics.csv"),
                 show_col_types = FALSE) %>%
    filter(stage == 2)

  # candidates_topics has no data_source column; the candidate name carries the
  # distinction through its capitalisation, so join on the name as given.
  theirs <- cx %>%
    left_join(tp %>% select(candidate, year, state, district, all_of(TOPICS)),
              by = c("candidate", "year", "state", "district")) %>%
    transmute(
      candidate, state, district = as.character(district), year,
      party,
      office = "house",
      n_char, n_words, n_tags, n_clean_tags,
      ttr = TTR, mattr = MATTR, entropy,
      across(all_of(TOPICS), identity),
      source = "icpsr")

  if (drop_icpsr_2018) {
    n_before <- nrow(theirs)
    theirs <- filter(theirs, year != 2018)
    n_dropped <- n_before - nrow(theirs)
    message(sprintf(paste("ICPSR 2018 general-election rows: %d.",
                          "(Their 76 stage-2 rows for 2018 all come from the",
                          "primary scrape, so the data_source filter removes",
                          "them already.)"), n_dropped))
  }

  # --- our side ---------------------------------------------------------
  ours_raw <- read_csv(ours_path, show_col_types = FALSE)
  topic_cols <- paste0("icpsr_topic_", TOPICS)
  missing <- setdiff(topic_cols, names(ours_raw))
  if (length(missing) > 0)
    stop("missing topic columns in ", ours_path, ": ",
         paste(head(missing, 3), collapse = ", "))

  ours <- ours_raw %>%
    transmute(
      candidate = candidate_icpsr, state,
      district = as.character(district_id), year,
      party,
      office,
      n_char = icpsr_n_char, n_words = icpsr_n_words,
      n_tags = icpsr_n_tags, n_clean_tags = icpsr_n_clean_tags,
      ttr = icpsr_ttr_approx, mattr = icpsr_mattr_approx,
      entropy = icpsr_entropy_approx,
      !!!setNames(lapply(topic_cols, function(x) rlang::sym(x)), TOPICS),
      source = "extension")

  # Their `party` is "democrat"/"republican"; ours is "D"/"R". Harmonise to
  # theirs, since a user merging into their years will expect their coding.
  ours <- ours %>%
    mutate(party = recode(as.character(party),
                          D = "democrat", R = "republican",
                          .default = as.character(party)))

  bind_rows(theirs, ours) %>% arrange(office, year, state, candidate)
}

# ---------------------------------------------------------------------------
# self-test
# ---------------------------------------------------------------------------
test_merge <- function(m) {
  ok <- TRUE
  chk <- function(label, pass, detail = "") {
    cat(sprintf("  [%s] %s%s\n", if (pass) "PASS" else "FAIL", label,
                if (nzchar(detail)) paste0(" -- ", detail) else ""))
    ok <<- ok && pass
  }
  cat("merged panel checks\n")

  chk("no duplicate candidate-years",
      !any(duplicated(m[c("candidate", "state", "district", "year", "office")])),
      sprintf("%d duplicates",
              sum(duplicated(m[c("candidate", "state", "district", "year", "office")]))))

  chk("both sources present", all(c("icpsr", "extension") %in% m$source),
      paste(names(table(m$source)), table(m$source), collapse = "; "))

  # Overlap must be checked WITHIN office. Our Senate 2002-2016 shares years
  # with their House data while sharing no candidate-years, which is fine.
  oy <- function(src) unique(paste(m$office[m$source == src], m$year[m$source == src]))
  dup_oy <- intersect(oy("icpsr"), oy("extension"))
  chk("no office-year appears in both sources", length(dup_oy) == 0,
      if (length(dup_oy)) paste(dup_oy, collapse = ", ") else "")

  tot <- rowSums(m[TOPICS], na.rm = FALSE)
  bad <- sum(!is.na(tot) & abs(tot - 1) > 1e-6)
  chk("topic proportions sum to 1", bad == 0, sprintf("%d rows off", bad))

  # The boundary is the reason this dataset needed a text-cleaning replication.
  # If the step were skipped, 2018 would jump ~50% above 2016.
  b <- m %>% filter(office == "house", year %in% c(2016, 2018)) %>%
    group_by(year) %>% summarise(med = median(n_char, na.rm = TRUE), .groups = "drop")
  if (nrow(b) == 2) {
    ratio <- b$med[b$year == 2018] / b$med[b$year == 2016]
    chk("no jump in document length at the 2016/2018 boundary",
        ratio > 0.8 && ratio < 1.25, sprintf("ratio %.3f", ratio))
  } else {
    chk("boundary years present", FALSE, "2016 or 2018 missing")
  }

  chk("senate years are extension-only",
      all(m$source[m$office == "senate"] == "extension"))

  cat(sprintf("\n%s\n", if (ok) "ALL CHECKS PASSED" else "SOME CHECKS FAILED"))
  invisible(ok)
}

# ---------------------------------------------------------------------------
if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  getarg <- function(flag, default = NULL) {
    i <- match(flag, args)
    if (is.na(i)) default else args[i + 1]
  }
  icpsr_dir <- getarg("--icpsr")
  ours <- getarg("--ours", "data/deliverable/panel_icpsr_compat.csv")
  out <- getarg("--out")
  if (is.null(icpsr_dir)) stop("give --icpsr <dir with candidates_complexity.csv>")

  m <- merge_icpsr(icpsr_dir, ours)
  cat(sprintf("merged: %d rows, %d columns\n", nrow(m), ncol(m)))
  print(m %>% count(source, office))

  if ("--test" %in% args) test_merge(m)
  if (!is.null(out)) {
    write_csv(m, out)
    cat(sprintf("wrote %s\n", out))
  }
}
