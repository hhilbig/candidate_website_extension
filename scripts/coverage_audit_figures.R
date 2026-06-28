#!/usr/bin/env Rscript
# Figures for the corpus coverage + quality audit.
# Reads tidy CSVs from quality_reports/coverage_audit/csv/, writes faceted
# vector PDFs to quality_reports/coverage_audit/figures/.
# Run from the repo root: Rscript scripts/coverage_audit_figures.R

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(tidyr)
  library(scales)
})

set.seed(20260628)
csv <- "quality_reports/coverage_audit/csv"
fig <- "quality_reports/coverage_audit/figures"
dir.create(fig, showWarnings = FALSE, recursive = TRUE)

py     <- read.csv(file.path(csv, "audit_per_year.csv"))
nchar_ <- read.csv(file.path(csv, "audit_nchar_sample.csv"))
ptype  <- read.csv(file.path(csv, "audit_pagetype.csv"))
month_ <- read.csv(file.path(csv, "audit_snap_month.csv"))
party_ <- read.csv(file.path(csv, "audit_by_party.csv"))
state_ <- read.csv(file.path(csv, "audit_by_state.csv"))

theme_set(theme_bw(base_size = 11) +
  theme(panel.grid.minor = element_blank(),
        strip.background = element_rect(fill = "grey92", colour = NA),
        legend.position = "top"))
oc <- scale_colour_manual(values = c(house = "#1b6ca8", senate = "#c0392b"))
of <- scale_fill_manual(values = c(house = "#1b6ca8", senate = "#c0392b"))

# ---- Figure 1: coverage ----
pdf(file.path(fig, "fig_coverage.pdf"), width = 9, height = 5.5)
print(
  ggplot(py, aes(year, capture_pct_validurl, colour = office)) +
    geom_line(linewidth = 0.7) + geom_point(size = 2) + oc +
    scale_x_continuous(breaks = seq(2002, 2024, 2)) +
    scale_y_continuous(limits = c(0, 100)) +
    labs(title = "Capture rate over time (candidates with >=1 snapshot)",
         subtitle = "Denominator = candidates with a valid campaign URL",
         x = NULL, y = "% of valid-URL candidates captured", colour = "Office")
)
gap <- py %>%
  transmute(office, year,
            `no valid URL` = no_url_n,
            `valid URL, no snapshot` = valid_no_capture_n,
            `captured` = captured_n) %>%
  pivot_longer(-c(office, year), names_to = "segment", values_to = "n") %>%
  mutate(segment = factor(segment,
           levels = c("captured", "valid URL, no snapshot", "no valid URL")))
print(
  ggplot(gap, aes(factor(year), n, fill = segment)) +
    geom_col(position = "fill") +
    facet_wrap(~office, scales = "free_x") +
    scale_fill_manual(values = c("captured" = "#2e7d32",
                                 "valid URL, no snapshot" = "#f0ad4e",
                                 "no valid URL" = "#b0b0b0")) +
    scale_y_continuous(labels = percent) +
    labs(title = "Coverage gap decomposition (share of roster)",
         subtitle = "Splits the un-captured remainder into URL-resolution vs Wayback-archival failure",
         x = "Election year", y = "Share of roster", fill = NULL)
)
invisible(dev.off())

# ---- Figure 2: quality (text richness + page types) ----
pdf(file.path(fig, "fig_quality.pdf"), width = 10, height = 6.5)
print(
  ggplot(nchar_ %>% filter(n_char > 0),
         aes(n_char, colour = office)) +
    geom_density() + oc +
    geom_vline(xintercept = 50, linetype = "dashed", colour = "grey40") +
    scale_x_log10(labels = comma) +
    facet_wrap(~ office + year, ncol = 4) +
    labs(title = "Page text richness (n_char) by office-year",
         subtitle = "Log scale; dashed line = 50-char low-text threshold",
         x = "Characters per page (log10)", y = "Density", colour = "Office")
)
pt <- ptype %>% group_by(office, year) %>% mutate(share = n_rows / sum(n_rows))
print(
  ggplot(pt, aes(factor(year), share, fill = page_type)) +
    geom_col() + facet_wrap(~office, scales = "free_x") +
    scale_y_continuous(labels = percent) +
    scale_fill_brewer(palette = "Set3") +
    labs(title = "Page-type composition by office-year",
         x = "Election year", y = "Share of pages", fill = "Page type")
)
invisible(dev.off())

# ---- Figure 3: snapshot density (the key consistency finding) ----
pdf(file.path(fig, "fig_snapshots.pdf"), width = 9, height = 6.5)
print(
  ggplot(py, aes(year, snaps_per_cand_mean, colour = office)) +
    geom_hline(yintercept = 4, linetype = "dashed", colour = "grey40") +
    geom_line(linewidth = 0.7) + geom_point(size = 2) + oc +
    annotate("text", x = 2003, y = 5.2, label = "3-month dedup target (~4)",
             hjust = 0, size = 3, colour = "grey30") +
    scale_x_continuous(breaks = seq(2002, 2024, 2)) +
    labs(title = "Mean snapshots per candidate (snapshot-density consistency)",
         subtitle = "Years near ~4 are dedup+cap normalized; higher = raw un-deduped CDX captures",
         x = NULL, y = "Mean snapshots / candidate", colour = "Office")
)
print(
  ggplot(py, aes(factor(year), snaps_per_cand_max, fill = office)) +
    geom_col() + of + facet_wrap(~office, scales = "free_x") +
    geom_hline(yintercept = 200, linetype = "dashed", colour = "grey40") +
    scale_y_log10(labels = comma) +
    labs(title = "Max snapshots for a single candidate (log scale)",
         subtitle = "Dashed line = 200-snapshot cap; bars far above it predate the cap",
         x = "Election year", y = "Max snapshots / candidate (log10)", fill = "Office")
)
invisible(dev.off())

# ---- Figure 4: temporal + selection ----
pdf(file.path(fig, "fig_temporal_selection.pdf"), width = 10, height = 6.5)
mm <- month_ %>% mutate(month = sprintf("%02d", as.integer(month)))
print(
  ggplot(mm, aes(month, n_snapshots, group = 1)) +
    geom_col(fill = "#1b6ca8") +
    facet_wrap(~ office + year, ncol = 4, scales = "free_y") +
    labs(title = "Snapshot timing within the election year",
         subtitle = "Counts by calendar month; quarterly clustering reflects the 3-month dedup buckets",
         x = "Month", y = "Snapshots")
)
pse <- party_ %>% filter(party %in% c("R", "D"))
print(
  ggplot(pse, aes(year, capture_pct_validurl, colour = party)) +
    geom_line(linewidth = 0.7) + geom_point(size = 1.8) +
    facet_wrap(~office, scales = "free_x") +
    scale_colour_manual(values = c(D = "#1f78b4", R = "#e31a1c")) +
    scale_x_continuous(breaks = seq(2002, 2024, 4)) +
    labs(title = "Capture rate by party (selection check)",
         subtitle = "Capture % of valid-URL candidates, Democrats vs Republicans",
         x = NULL, y = "% captured", colour = "Party")
)
st <- state_ %>% group_by(office, state) %>%
  summarise(cap = sum(captured_n), ros = sum(roster_n), .groups = "drop") %>%
  mutate(rate = 100 * cap / ros)
print(
  ggplot(st, aes(rate, reorder(state, rate))) +
    geom_point(aes(colour = office), size = 1.6) + oc +
    facet_wrap(~office) +
    labs(title = "Capture rate by state (pooled across years)",
         subtitle = "Geographic coverage gaps; states ordered by rate",
         x = "% of roster captured", y = NULL, colour = "Office") +
    theme(axis.text.y = element_text(size = 5))
)
invisible(dev.off())

cat("Figures written to", fig, "\n")
for (f in list.files(fig, pattern = "pdf$", full.names = TRUE))
  cat(sprintf("  %s (%d bytes)\n", basename(f), file.info(f)$size))
