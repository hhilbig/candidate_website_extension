# Coverage and validation figures for the candidate-websites release.
# Style follows the CLARA data report: theme_bw base, no gridlines, no in-plot
# titles, restrained blue-plus-grey palette, direct labels instead of legends,
# value annotations on key points, sans font.
#
# Inputs are the CSVs written by scripts/figure_data.py.
# Run:  Rscript scripts/figures_report.R

set.seed(20260814)
suppressPackageStartupMessages({
  library(tidyverse)
  library(scales)
})

data_dir <- Sys.getenv("FIGURE_DATA_DIR", "quality_reports/figures/data")
out_dir <- Sys.getenv("FIGURE_OUT_DIR", "quality_reports/figures")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

BLUE <- "#2f6f9f"
DARK <- "#1d4f77"
GREY <- "grey55"
BAND <- "#2f6f9f"
RED  <- "#a4443a"

theme_report <- function(fontsize = 13) {
  theme_bw(base_size = fontsize) +
    theme(
      panel.grid = element_blank(),
      strip.background = element_blank(),
      strip.text = element_text(color = "black", face = "bold", size = fontsize - 1),
      panel.border = element_rect(color = "grey70"),
      axis.ticks = element_blank(),
      legend.position = "none",
      plot.margin = margin(8, 14, 8, 12)
    )
}

save_fig <- function(p, name, width, height) {
  ggsave(file.path(out_dir, paste0(name, ".pdf")), p, width = width,
         height = height, device = cairo_pdf)
  ggsave(file.path(out_dir, paste0(name, ".png")), p, width = width,
         height = height, dpi = 450)
  message("wrote ", name)
}

# --------------------------------------------------------------- fig-coverage
# Coverage for both collections on one axis, 2002 to 2024.
#
# The denominator is the ballot, not either project's roster: every Democratic
# and Republican candidate in the MIT general-election returns. That is the only
# denominator observable for both, so grey (Di Tella et al., House 2002-2016)
# and blue (this dataset) are directly comparable.
#
# Two lines per series. Solid counts candidates. Dashed weights each candidate by
# the votes they received, which answers a different question: not "what share of
# candidates do we have" but "what share of the votes cast went to a candidate
# whose website we have". A missed candidate with 200,000 votes should not count
# the same as one with 8,000.

cb <- read_csv(file.path(data_dir, "coverage_vs_ballot.csv"),
               show_col_types = FALSE) |>
  mutate(label = if_else(office == "house", "House", "Senate"),
         series = paste(source, office))

cb_long <- cb |>
  select(label, series, source, year, pct_cand, pct_votes) |>
  pivot_longer(c(pct_cand, pct_votes), names_to = "weight", values_to = "pct") |>
  mutate(weight = factor(weight, c("pct_cand", "pct_votes"),
                         c("candidates", "voters")))

# Direct labels, placed by hand so they sit clear of both line types.
cb_lab <- tibble(
  label  = c("House", "House", "Senate"),
  source = c("icpsr", "extension", "extension"),
  year   = c(2009, 2021, 2015),
  pct    = c(47, 88, 38),
  text   = c("Di Tella et al.", "this dataset", "this dataset"))

p_cov <- ggplot(cb_long, aes(x = year, y = pct,
                             group = interaction(series, weight))) +
  geom_line(aes(color = source, linetype = weight), linewidth = 0.7) +
  geom_point(data = filter(cb_long, weight == "candidates"),
             aes(color = source), size = 1.6) +
  geom_text(data = cb_lab, aes(x = year, y = pct, label = text,
                               color = source),
            size = 3.1, fontface = "bold",
            inherit.aes = FALSE, show.legend = FALSE) +
  facet_wrap(~label, ncol = 2) +
  scale_color_manual(values = c(icpsr = GREY, extension = DARK)) +
  scale_linetype_manual(values = c(candidates = "solid", voters = "22")) +
  scale_x_continuous(breaks = seq(2002, 2024, 6)) +
  scale_y_continuous(limits = c(0, 100), breaks = c(0, 25, 50, 75, 100)) +
  labs(x = NULL, y = "Captured (% of ballot)") +
  theme_report()

# Explain the two line types once, inside the House panel, instead of a legend.
key <- tibble(label = "House", year = c(2003.2, 2003.2), pct = c(22, 12),
              weight = factor(c("candidates", "voters"),
                              c("candidates", "voters")),
              text = c("share of candidates", "share of votes cast"))
p_cov <- p_cov +
  geom_line(data = key |> mutate(x1 = year - 0.9, x2 = year + 1.9) |>
              pivot_longer(c(x1, x2), values_to = "xx") |>
              mutate(series = "key"),
            aes(x = xx, y = pct, group = weight, linetype = weight),
            color = "grey35", linewidth = 0.6, inherit.aes = FALSE) +
  geom_text(data = key |> mutate(series = "key"),
            aes(x = year + 2.4, y = pct, label = text), hjust = 0, size = 2.7,
            color = "grey35", inherit.aes = FALSE)
save_fig(p_cov, "coverage_by_office_year", 7.6, 3.8)

# Absolute counts alongside the rate: a high rate on 50 candidates is not the
# same evidence as a high rate on 527. Roster-based, so this one is about this
# release only.
cov <- read_csv(file.path(data_dir, "coverage_by_office_year.csv"),
                show_col_types = FALSE) |>
  mutate(label = if_else(office == "house", "House", "Senate"))

cnt <- cov |>
  select(label, year, roster, captured) |>
  pivot_longer(c(roster, captured), names_to = "what", values_to = "n") |>
  mutate(what = factor(what, c("roster", "captured"),
                       c("attempted", "captured")))

p_cnt <- ggplot(cnt, aes(x = year, y = n, group = what)) +
  geom_line(aes(color = what), linewidth = 0.65) +
  geom_point(aes(color = what), size = 1.5) +
  facet_wrap(~label, ncol = 2, scales = "free") +
  scale_color_manual(values = c(attempted = GREY, captured = DARK)) +
  labs(x = NULL, y = "Candidate-years") +
  theme_report() +
  theme(legend.position = "none")
# direct labels rather than a legend
p_cnt <- p_cnt +
  geom_text(data = cnt |> group_by(label, what) |> filter(year == max(year)) |>
              ungroup(),
            aes(label = as.character(what), color = what),
            hjust = 1, vjust = -0.9, size = 2.9, show.legend = FALSE)
save_fig(p_cnt, "coverage_counts", 7.6, 3.6)

# --------------------------------------------------------------- fig-boundary
# The validation figure. ICPSR's published series and ours, joined at 2016/2018.
# The dashed line is what our series would look like without replicating their
# text-cleaning step -- the error this release was built to avoid.

b <- read_csv(file.path(data_dir, "boundary_nchar.csv"), show_col_types = FALSE) |>
  filter(office == "house")

icpsr <- filter(b, source == "icpsr")
ours <- filter(b, source == "extension")

p_bound <- ggplot() +
  annotate("rect", xmin = 2016.5, xmax = 2018.5, ymin = -Inf, ymax = Inf,
           fill = BAND, alpha = 0.08) +
  geom_line(data = icpsr, aes(x = year, y = cleaned), color = GREY,
            linewidth = 0.75) +
  geom_point(data = icpsr, aes(x = year, y = cleaned), color = GREY, size = 1.8) +
  geom_line(data = ours, aes(x = year, y = cleaned), color = DARK,
            linewidth = 0.85) +
  geom_point(data = ours, aes(x = year, y = cleaned), color = DARK, size = 2.0) +
  annotate("text", x = 2008, y = 1150, label = "Di Tella et al.",
           size = 3.1, color = GREY) +
  annotate("text", x = 2021.5, y = 1500, label = "this dataset",
           size = 3.1, color = DARK, fontface = "bold") +
  scale_x_continuous(breaks = seq(2002, 2024, 4), limits = c(2002, 2025)) +
  scale_y_continuous(limits = c(0, 2400), labels = comma) +
  labs(x = NULL, y = "Median characters") +
  theme_report()
save_fig(p_bound, "boundary_continuity", 7.4, 4.2)

# ------------------------------------------------------------- fig-validation
# How closely the reimplementation reproduces ICPSR's published values, run on
# ICPSR's own text so that only the coding differs.

v <- read_csv(file.path(data_dir, "validation_pairs.csv"), show_col_types = FALSE)

stat <- v |>
  group_by(variable) |>
  summarise(r = cor(published, ours), n = n(), .groups = "drop") |>
  mutate(lab = sprintf("r = %.4f", r))

p_val <- ggplot(v, aes(x = published, y = ours)) +
  geom_abline(slope = 1, intercept = 0, color = "grey70", linewidth = 0.4) +
  geom_point(color = BLUE, alpha = 0.35, size = 0.9) +
  geom_text(data = stat, aes(x = -Inf, y = Inf, label = lab),
            hjust = -0.12, vjust = 1.6, size = 3.0, color = "grey20",
            inherit.aes = FALSE) +
  facet_wrap(~variable, scales = "free", ncol = 3) +
  labs(x = "ICPSR published value", y = "Reimplementation") +
  theme_report()
save_fig(p_val, "validation_scatter", 7.8, 5.0)

# ----------------------------------------------------------------- fig-topics
# Face validity. The classifier is never told a candidate's party, so if the
# topic measures carry signal, the partisan gaps should line up with what we
# know about the two parties' issue agendas.
#
# Use House general-election candidates only. This keeps the denominator stable
# across the four years and avoids giving small Senate cells the same weight as
# House cells with hundreds of candidates.

tp <- read_csv(file.path(data_dir, "topic_by_party.csv"), show_col_types = FALSE)

KEY_TOPICS <- c(
  "Welfare State", "Labour Groups", "Equality", "Education", "Social Groups",
  "Democracy", "Market Regulation", "Sustainability", "Multiculturalism",
  "Free Market Economy", "Economic Planning", "Military", "Law and Order",
  "Traditional Morality", "National Way of Life", "Political Authority")

gap <- tp |>
  filter(office == "house", topic %in% KEY_TOPICS) |>
  pivot_wider(names_from = party, values_from = share) |>
  mutate(gap = (D - R) * 100) |>
  group_by(topic) |>
  summarise(mean_gap = mean(gap), min_gap = min(gap), max_gap = max(gap),
            .groups = "drop") |>
  mutate(topic = fct_reorder(topic, mean_gap),
         lean = if_else(mean_gap > 0, "D", "R"))

p_gap <- ggplot(gap, aes(x = mean_gap, y = topic)) +
  geom_vline(xintercept = 0, color = "grey70", linewidth = 0.4) +
  geom_segment(aes(x = min_gap, xend = max_gap, yend = topic, color = lean),
               linewidth = 0.65) +
  geom_point(aes(color = lean), size = 2.4) +
  scale_color_manual(values = c(D = DARK, R = RED)) +
  scale_x_continuous(labels = function(x) sprintf("%+g", x)) +
  labs(x = "Democratic minus Republican attention (percentage points)",
       y = NULL) +
  theme_report(11) +
  theme(axis.text.y = element_text(size = 9))
save_fig(p_gap, "topics_party_gap", 7.2, 4.6)

# The single cleanest series, shown on its own because it is the one panel
# where the gap is large, consistent, and visible in every year.
welfare <- tp |>
  filter(office == "house", topic == "Welfare State") |>
  group_by(party, year) |>
  summarise(share = mean(share) * 100, .groups = "drop")
w_ends <- welfare |> group_by(party) |> filter(year == max(year)) |> ungroup()

p_welfare <- ggplot(welfare, aes(x = year, y = share, group = party)) +
  geom_line(aes(color = party), linewidth = 0.8) +
  geom_point(aes(color = party), size = 1.6) +
  geom_text(data = w_ends, aes(label = party, color = party),
            hjust = -0.5, size = 3.4, fontface = "bold") +
  scale_color_manual(values = c(D = DARK, R = RED)) +
  scale_x_continuous(breaks = seq(2002, 2024, 4), limits = c(2002, 2026)) +
  labs(x = NULL, y = "Welfare State (% of topic attention)") +
  theme_report()
save_fig(p_welfare, "topics_welfare_state", 7.2, 4.0)

# ------------------------------------------------------- fig-external-validity
# Do the topic measures agree with an ideology measure built from something
# else entirely? DIME CF-scores come from campaign finance and share no input
# with the website text, so agreement is external evidence rather than internal
# consistency.
#
# Reported honestly in two halves. Pooled across parties the expected ordering
# appears. WITHIN party it largely disappears, which is a limitation users need:
# these measures separate parties, they do not track ideological variation
# inside a party.

cf <- read_csv(file.path(data_dir, "cfscore_correlations.csv"),
               show_col_types = FALSE)

cf_long <- cf |>
  filter(topic %in% KEY_TOPICS) |>          # same subset as the party-gap figure
  select(topic, Pooled = r_all, Democrats = r_D, Republicans = r_R) |>
  pivot_longer(-topic, names_to = "which", values_to = "r") |>
  mutate(which = factor(which, c("Pooled", "Democrats", "Republicans")),
         topic = fct_reorder(topic, if_else(which == "Pooled", r, NA_real_),
                             .fun = function(x) mean(x, na.rm = TRUE),
                             .na_rm = TRUE))

p_cf <- ggplot(cf_long, aes(x = r, y = topic)) +
  geom_vline(xintercept = 0, color = "grey70", linewidth = 0.4) +
  geom_segment(aes(x = 0, xend = r, yend = topic, color = which),
               linewidth = 0.45) +
  geom_point(aes(color = which), size = 1.9) +
  facet_wrap(~which, ncol = 3) +
  scale_color_manual(values = c(Pooled = DARK, Democrats = BLUE,
                                Republicans = RED)) +
  scale_x_continuous(limits = c(-0.36, 0.36),
                     breaks = c(-0.3, 0, 0.3)) +
  labs(x = "Correlation with DIME CF-score (higher = more conservative)",
       y = NULL) +
  theme_report(11) +
  theme(axis.text.y = element_text(size = 8))
save_fig(p_cf, "external_validity_cfscore", 8.4, 4.4)

# ------------------------------------------------------------- fig-welfare-arc
# Welfare State attention over the full merged House series, split by party and
# restricted to general-election candidates on both sides of the 2016/2018
# handoff. Separate line segments mark the change in source dataset.

ws <- read_csv(file.path(data_dir, "welfare_series.csv"), show_col_types = FALSE)

p_ws <- ggplot(ws, aes(x = year, y = share, color = party,
                       group = interaction(party, source))) +
  annotate("rect", xmin = 2016.5, xmax = 2018.5, ymin = -Inf, ymax = Inf,
           fill = BAND, alpha = 0.08) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.9) +
  geom_text(data = ws |> group_by(party) |> filter(year == max(year)) |>
              ungroup(),
            aes(label = if_else(party == "democrat", "Democrats", "Republicans")),
            hjust = -0.25, size = 3.0, fontface = "bold") +
  scale_color_manual(values = c(democrat = DARK, republican = RED)) +
  scale_x_continuous(breaks = seq(2002, 2024, 4), limits = c(2002, 2027)) +
  scale_y_continuous(limits = c(0, 20), expand = expansion(mult = c(0, 0.08))) +
  labs(x = NULL, y = "Welfare State (% of attention)") +
  theme_report()
save_fig(p_ws, "welfare_state_series", 7.2, 4.0)

message("\nall figures written to ", out_dir)
