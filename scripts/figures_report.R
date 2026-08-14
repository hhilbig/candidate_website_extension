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

data_dir <- "quality_reports/figures/data"
out_dir <- "quality_reports/figures"
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
      plot.margin = margin(8, 14, 8, 8)
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
# What share of the candidates we attempted actually yielded text, by
# office-year. The point of the figure is that coverage is neither complete nor
# constant, and is thinnest in the early Senate years.

cov <- read_csv(file.path(data_dir, "coverage_by_office_year.csv"),
                show_col_types = FALSE) |>
  mutate(label = if_else(office == "house", "House", "Senate"))

lab_pts <- cov |>
  group_by(office) |>
  filter(year == min(year) | year == max(year)) |>
  ungroup()

p_cov <- ggplot(cov, aes(x = year, y = pct_of_url)) +
  geom_line(color = BLUE, linewidth = 0.65) +
  geom_point(color = BLUE, size = 1.8) +
  geom_text(data = lab_pts, aes(label = sprintf("%.0f", pct_of_url)),
            vjust = -1.1, size = 3.0, color = "grey20") +
  facet_wrap(~label, ncol = 2, scales = "free_x") +
  scale_y_continuous(limits = c(0, 100), breaks = c(0, 25, 50, 75, 100)) +
  labs(x = NULL,
       y = "Captured (% of candidates with a usable URL)") +
  theme_report()
save_fig(p_cov, "coverage_by_office_year", 7.6, 3.6)

# Absolute counts alongside the rate: a high rate on 72 candidates is not the
# same evidence as a high rate on 693.
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
  annotate("text", x = 2017.5, y = 2750, label = "boundary",
           size = 2.9, color = "grey45") +
  geom_line(data = ours, aes(x = year, y = uncleaned), color = RED,
            linewidth = 0.6, linetype = "22") +
  geom_point(data = ours, aes(x = year, y = uncleaned), color = RED, size = 1.6) +
  annotate("text", x = 2024.2, y = tail(ours$uncleaned, 1),
           label = "without replicating\ntheir text cleaning", hjust = 0,
           size = 2.9, color = RED, lineheight = 0.95) +
  geom_line(data = icpsr, aes(x = year, y = cleaned), color = GREY,
            linewidth = 0.75) +
  geom_point(data = icpsr, aes(x = year, y = cleaned), color = GREY, size = 1.8) +
  geom_line(data = ours, aes(x = year, y = cleaned), color = DARK,
            linewidth = 0.85) +
  geom_point(data = ours, aes(x = year, y = cleaned), color = DARK, size = 2.0) +
  annotate("text", x = 2009, y = 1180, label = "ICPSR 226001",
           size = 3.1, color = GREY) +
  annotate("text", x = 2021, y = 1430, label = "this dataset",
           size = 3.1, color = DARK, fontface = "bold") +
  scale_x_continuous(breaks = seq(2002, 2024, 4), limits = c(2002, 2028)) +
  scale_y_continuous(limits = c(0, 3000), labels = comma) +
  labs(x = NULL, y = "Median characters per candidate-year") +
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
# Plotting the six topic time series directly is a poor test: before 2018 our
# data is Senate-only, and the 2002 Senate has 30 captured candidates, so the
# early years swing wildly on tiny samples. Instead take the Democratic minus
# Republican gap within each of the 16 office-years, and show its mean and
# range across them. Direction AND consistency in one figure, using all 31
# topics rather than a chosen few.

tp <- read_csv(file.path(data_dir, "topic_by_party.csv"), show_col_types = FALSE)

gap <- tp |>
  pivot_wider(names_from = party, values_from = share) |>
  mutate(gap = (D - R) * 100) |>
  group_by(topic) |>
  summarise(mean_gap = mean(gap), lo = min(gap), hi = max(gap),
            consistent = mean(sign(gap) == sign(mean(gap))), .groups = "drop") |>
  mutate(topic = fct_reorder(topic, mean_gap),
         lean = if_else(mean_gap > 0, "D", "R"))

p_gap <- ggplot(gap, aes(y = topic)) +
  geom_vline(xintercept = 0, color = "grey70", linewidth = 0.4) +
  geom_linerange(aes(xmin = lo, xmax = hi, color = lean),
                 linewidth = 0.5, alpha = 0.35) +
  geom_point(aes(x = mean_gap, color = lean), size = 2.1) +
  scale_color_manual(values = c(D = DARK, R = RED)) +
  scale_x_continuous(labels = function(x) sprintf("%+g", x)) +
  labs(x = "Democratic minus Republican attention (percentage points)",
       y = NULL) +
  theme_report(11) +
  theme(axis.text.y = element_text(size = 8.5))
save_fig(p_gap, "topics_party_gap", 7.4, 6.4)

# The single cleanest series, shown on its own because it is the one panel
# where the gap is large, consistent, and visible in every year.
welfare <- tp |>
  filter(topic == "Welfare State") |>
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

message("\nall figures written to ", out_dir)

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
  select(topic, Pooled = r_all, Democrats = r_D, Republicans = r_R) |>
  pivot_longer(-topic, names_to = "which", values_to = "r") |>
  mutate(which = factor(which, c("Pooled", "Democrats", "Republicans")),
         topic = fct_reorder(topic, if_else(which == "Pooled", r, NA_real_),
                             .fun = function(x) mean(x, na.rm = TRUE)))

p_cf <- ggplot(cf_long, aes(x = r, y = topic)) +
  geom_vline(xintercept = 0, color = "grey70", linewidth = 0.4) +
  geom_point(aes(color = which), size = 1.9) +
  facet_wrap(~which, ncol = 3) +
  scale_color_manual(values = c(Pooled = DARK, Democrats = BLUE,
                                Republicans = RED)) +
  scale_x_continuous(limits = c(-0.32, 0.32),
                     breaks = c(-0.25, 0, 0.25)) +
  labs(x = "Correlation with DIME CF-score (higher = more conservative)",
       y = NULL) +
  theme_report(11) +
  theme(axis.text.y = element_text(size = 8))
save_fig(p_cf, "external_validity_cfscore", 8.4, 6.4)

# ------------------------------------------------------------- fig-welfare-arc
# Welfare State attention over the full merged series. Shown only for a LARGE
# topic: the two classifier runs differ by about 0.005 in absolute terms, which
# is negligible against a series running 4-9% but comparable to the level of a
# topic like Multiculturalism at 1.4%. Small topics therefore cannot support
# cross-boundary trend claims, and none are shown here.

ws <- read_csv(file.path(data_dir, "welfare_series.csv"), show_col_types = FALSE)

p_ws <- ggplot(ws, aes(x = year, y = share)) +
  annotate("rect", xmin = 2019.4, xmax = 2020.6, ymin = -Inf, ymax = Inf,
           fill = BAND, alpha = 0.10) +
  annotate("text", x = 2020, y = 3.2, label = "COVID-19", size = 2.8,
           color = "grey45") +
  geom_line(aes(group = source, color = source), linewidth = 0.8) +
  geom_point(aes(color = source), size = 1.9) +
  annotate("text", x = 2008, y = 3.2, label = "ICPSR 226001",
           size = 3.0, color = GREY) +
  annotate("text", x = 2022, y = 6.6, label = "this dataset",
           size = 3.0, color = DARK, fontface = "bold") +
  scale_color_manual(values = c(icpsr = GREY, extension = DARK)) +
  scale_x_continuous(breaks = seq(2002, 2024, 4)) +
  scale_y_continuous(limits = c(2.5, 10.5)) +
  labs(x = NULL, y = "Welfare State (% of topic attention, House)") +
  theme_report()
save_fig(p_ws, "welfare_state_series", 7.2, 4.0)
