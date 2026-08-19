# Internal candidate figures for the public dataset announcement.

set.seed(20260819)
suppressPackageStartupMessages({
  library(tidyverse)
})

data_dir <- Sys.getenv(
  "SOCIAL_FIGURE_DATA_DIR",
  "quality_reports/social_figure_candidates/data"
)
out_dir <- Sys.getenv(
  "SOCIAL_FIGURE_OUT_DIR",
  "quality_reports/social_figure_candidates/figures"
)
report_data_dir <- Sys.getenv(
  "REPORT_FIGURE_DATA_DIR",
  "quality_reports/figures/data"
)
carousel_dir <- Sys.getenv(
  "RELEASE_CAROUSEL_DIR",
  "quality_reports/social_figure_candidates/release_carousel"
)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(carousel_dir, showWarnings = FALSE, recursive = TRUE)

BLUE <- "#2f6f9f"
DARK <- "#1d4f77"
GREY <- "grey55"
BAND <- "#2f6f9f"
RED <- "#a4443a"

theme_social <- function(fontsize = 13) {
  theme_bw(base_size = fontsize) +
    theme(
      panel.grid = element_blank(),
      strip.background = element_blank(),
      strip.text = element_text(color = "black", face = "bold", size = fontsize - 1),
      panel.border = element_rect(color = "grey70"),
      axis.ticks = element_blank(),
      legend.position = "none",
      plot.margin = margin(8, 14, 8, 8),
      plot.title = element_text(face = "bold", size = 15, hjust = 0)
    )
}

save_carousel <- function(plot, name) {
  ggsave(file.path(carousel_dir, paste0(name, ".png")), plot,
         width = 7.6, height = 4.8, dpi = 450)
  message("wrote carousel/", name)
}

save_candidate <- function(plot, name, width, height) {
  ggsave(file.path(out_dir, paste0(name, ".png")), plot,
         width = width, height = height, dpi = 450)
  ggsave(file.path(out_dir, paste0(name, ".pdf")), plot,
         width = width, height = height, device = cairo_pdf)
  message("wrote ", name)
}

gaps <- read_csv(file.path(data_dir, "long_run_topic_gaps.csv"),
                 show_col_types = FALSE) |>
  mutate(
    topic = factor(topic, c("Welfare State", "Equality", "Law and Order", "Military")),
    direction = if_else(gap >= 0, "More Democratic", "More Republican")
  )

p1 <- ggplot(gaps, aes(year, gap, group = source, color = source)) +
  annotate("rect", xmin = 2016.5, xmax = 2017.5,
           ymin = -Inf, ymax = Inf, fill = BAND, alpha = 0.10, color = NA) +
  geom_hline(yintercept = 0, color = "grey70", linewidth = 0.4) +
  geom_line(linewidth = 0.65) +
  geom_point(size = 1.7) +
  facet_wrap(~topic, scales = "free_y", ncol = 2) +
  scale_color_manual(values = c("Di Tella et al." = GREY,
                                "This release" = DARK)) +
  scale_x_continuous(breaks = seq(2002, 2024, 6)) +
  labs(title = "How Party Issue Emphasis Changed, 2002–2024",
       x = NULL, y = "Democratic minus Republican attention (pp)") +
  theme_social()
save_candidate(p1, "proposal_01_long_run_partisan_gaps", 7.6, 4.8)
save_carousel(p1, "01_long_run_partisan_gaps")

a24 <- read_csv(file.path(data_dir, "agenda_2024.csv"), show_col_types = FALSE) |>
  mutate(
    gap = democrat - republican,
    topic = fct_reorder(topic, gap)
  )

p2 <- ggplot(a24, aes(y = topic)) +
  geom_segment(aes(x = democrat, xend = republican, yend = topic),
               color = "grey85", linewidth = 0.9) +
  geom_point(aes(x = democrat), color = DARK, size = 2.6) +
  geom_point(aes(x = republican), color = RED, size = 2.6) +
  geom_text(data = filter(a24, topic == "Welfare State"),
            aes(x = democrat, label = "Democrats"),
            color = DARK, nudge_y = 0.28, hjust = 1.08,
            size = 3.0, fontface = "bold") +
  geom_text(data = filter(a24, topic == "Welfare State"),
            aes(x = republican, label = "Republicans"),
            color = RED, nudge_y = 0.28, size = 3.0) +
  labs(title = "What the Parties Emphasized in 2024",
       x = "Share of topic attention (%)", y = NULL) +
  theme_social()
save_candidate(p2, "proposal_02_2024_agenda", 7.6, 4.8)
save_carousel(p2, "03_2024_party_agendas")

coverage <- read_csv(file.path(report_data_dir, "coverage_vs_ballot.csv"),
                     show_col_types = FALSE) |>
  mutate(
    chamber = if_else(office == "house", "House", "Senate"),
    series = paste(source, office)
  ) |>
  select(chamber, series, source, year, pct_cand, pct_votes) |>
  pivot_longer(c(pct_cand, pct_votes),
               names_to = "weight", values_to = "pct") |>
  mutate(weight = factor(weight, c("pct_cand", "pct_votes"),
                         c("Candidates", "Votes cast")))

coverage_key <- tibble(
  chamber = "House",
  year = c(2003.2, 2003.2),
  pct = c(22, 12),
  weight = factor(c("Candidates", "Votes cast"),
                  c("Candidates", "Votes cast")),
  text = c("Share of candidates", "Share of votes cast")
)

p_coverage <- ggplot(
  coverage,
  aes(year, pct, group = interaction(series, weight))
) +
  geom_line(aes(color = source, linetype = weight), linewidth = 0.7) +
  geom_point(data = filter(coverage, weight == "Candidates"),
             aes(color = source), size = 1.6) +
  geom_line(
    data = coverage_key |>
      mutate(x1 = year - 0.9, x2 = year + 1.9) |>
      pivot_longer(c(x1, x2), values_to = "xx") |>
      mutate(series = "key"),
    aes(x = xx, y = pct, group = weight, linetype = weight),
    color = "grey35", linewidth = 0.6, inherit.aes = FALSE
  ) +
  geom_text(
    data = coverage_key,
    aes(x = year + 2.4, y = pct, label = text),
    hjust = 0, size = 2.7, color = "grey35", inherit.aes = FALSE
  ) +
  facet_wrap(~chamber, ncol = 2) +
  scale_color_manual(values = c(icpsr = GREY, extension = DARK)) +
  scale_linetype_manual(values = c("Candidates" = "solid",
                                   "Votes cast" = "22")) +
  scale_x_continuous(breaks = seq(2002, 2024, 6)) +
  scale_y_continuous(limits = c(0, 100),
                     breaks = c(0, 25, 50, 75, 100)) +
  labs(title = "Website Coverage Against the Official Ballot",
       x = NULL, y = "Captured (% of ballot)") +
  theme_social()
save_carousel(p_coverage, "02_ballot_coverage")

development <- read_csv(
  file.path(data_dir, "site_development_by_race_margin.csv"),
  show_col_types = FALSE
) |>
  mutate(margin_bin = factor(
    margin_bin,
    c("0–10", "10–20", "20–30", "30–40", "40–60", "60–100")
  ))

p_development <- ggplot(
  development,
  aes(margin_bin, share_developed, group = 1)
) +
  geom_line(color = DARK, linewidth = 0.75) +
  geom_point(color = DARK, size = 2.2) +
  scale_y_continuous(
    limits = c(0, 60), breaks = c(0, 20, 40, 60),
    labels = function(x) paste0(x, "%")
  ) +
  labs(
    title = "Closer Races Had Fuller Campaign Websites",
    x = "Two-party vote margin (percentage points)",
    y = "Sites with at least 3 page types (%)"
  ) +
  theme_social()
save_carousel(p_development, "04_race_competitiveness")

distance <- read_csv(file.path(data_dir, "agenda_distance.csv"),
                     show_col_types = FALSE)
p3 <- ggplot(distance, aes(year, total_variation_pp, group = source)) +
  annotate("rect", xmin = 2016.5, xmax = 2017.5,
           ymin = -Inf, ymax = Inf, fill = BAND, alpha = 0.10, color = NA) +
  geom_line(color = DARK, linewidth = 0.65) +
  geom_point(color = DARK, size = 1.7) +
  geom_text(data = filter(distance, year %in% c(2002, 2016, 2018, 2024)),
            aes(label = sprintf("%.1f", total_variation_pp)),
            vjust = -0.8, size = 3.0, color = "grey20") +
  scale_x_continuous(breaks = seq(2002, 2024, 4)) +
  scale_y_continuous(limits = c(0, max(distance$total_variation_pp) * 1.15)) +
  labs(x = NULL, y = "Party-agenda distance (pp)") +
  theme_social()
save_candidate(p3, "proposal_03_party_agenda_distance", 7.2, 4.2)

chamber <- read_csv(file.path(data_dir, "chamber_replication.csv"),
                    show_col_types = FALSE) |>
  mutate(label = if_else(topic %in% c(
    "Welfare State", "Political Authority", "Equality", "Law and Order",
    "Military", "Education", "National Way of Life", "Labour Groups"
  ), topic, NA_character_))

p4 <- ggplot(chamber, aes(house, senate)) +
  geom_abline(slope = 1, intercept = 0, color = "grey75", linewidth = 0.5) +
  geom_hline(yintercept = 0, color = "grey85", linewidth = 0.35) +
  geom_vline(xintercept = 0, color = "grey85", linewidth = 0.35) +
  geom_point(color = DARK, size = 1.7, alpha = 0.85) +
  geom_text(data = filter(chamber, !is.na(label)), aes(label = label),
            size = 2.7, nudge_y = 0.35,
            check_overlap = TRUE) +
  coord_equal(xlim = c(-5.5, 11.5), ylim = c(-7.5, 14.5)) +
  labs(x = "House D–R gap (pp)", y = "Senate D–R gap (pp)") +
  theme_social(12)
save_candidate(p4, "proposal_04_chamber_replication", 6.4, 5.6)

home <- read_csv(file.path(data_dir, "homepage_replication.csv"),
                 show_col_types = FALSE) |>
  mutate(label = if_else(topic %in% c(
    "Welfare State", "Political Authority", "Equality", "Law and Order",
    "Military", "Education", "National Way of Life", "Labour Groups"
  ), topic, NA_character_))

p5 <- ggplot(home, aes(full_gap, home_gap)) +
  geom_abline(slope = 1, intercept = 0, color = "grey75", linewidth = 0.5) +
  geom_hline(yintercept = 0, color = "grey85", linewidth = 0.35) +
  geom_vline(xintercept = 0, color = "grey85", linewidth = 0.35) +
  geom_point(color = DARK, size = 1.7, alpha = 0.85) +
  geom_text(data = filter(home, !is.na(label)), aes(label = label),
            size = 2.7, nudge_y = 0.3,
            check_overlap = TRUE) +
  coord_equal(xlim = c(-5.8, 7.2), ylim = c(-5.2, 4.4)) +
  labs(x = "Full-site D–R gap (pp)", y = "Homepage D–R gap (pp)") +
  theme_social(12)
save_candidate(p5, "proposal_05_homepage_replication", 6.4, 5.6)

displacement <- read_csv(file.path(data_dir, "home_full_displacement.csv"),
                         show_col_types = FALSE) |>
  group_by(topic) |>
  summarise(value = mean(full_minus_home_pp), .groups = "drop") |>
  slice_max(abs(value), n = 14) |>
  mutate(topic = fct_reorder(topic, value),
         direction = if_else(value > 0, "Deeper in site", "More on homepage"))

p6 <- ggplot(displacement, aes(value, topic)) +
  geom_vline(xintercept = 0, color = "grey45", linewidth = 0.4,
             linetype = "dotted") +
  geom_segment(aes(x = 0, xend = value, yend = topic),
               color = "grey85", linewidth = 0.9) +
  geom_point(size = 2.6, color = DARK) +
  labs(x = "Full site minus homepage attention (pp)", y = NULL) +
  theme_social(11)
save_candidate(p6, "proposal_06_homepage_vs_full_site", 7.2, 4.8)

lengths <- read_csv(file.path(data_dir, "website_length_by_party.csv"),
                    show_col_types = FALSE) |>
  mutate(party = recode(party,
                        democrat = "Democrats",
                        republican = "Republicans"))

length_labels <- lengths |>
  filter(year == 2024) |>
  mutate(
    label = sprintf("%s: %.0f", party, median_words),
    label_y = median_words + if_else(party == "Democrats", 18, -18)
  )

p7 <- ggplot(lengths,
             aes(year, median_words, group = interaction(source, party),
                 color = party)) +
  annotate("rect", xmin = 2016.5, xmax = 2017.5,
           ymin = -Inf, ymax = Inf, fill = BAND, alpha = 0.10, color = NA) +
  geom_line(linewidth = 0.65) +
  geom_point(size = 1.7) +
  geom_text(data = length_labels,
            aes(y = label_y, label = label),
            color = "grey20", hjust = 1.05, size = 3.1) +
  scale_color_manual(values = c("Democrats" = DARK,
                                "Republicans" = GREY)) +
  scale_x_continuous(breaks = seq(2002, 2024, 6),
                     limits = c(2002, 2025)) +
  scale_y_continuous(breaks = seq(0, 400, 100),
                     limits = c(0, 400)) +
  labs(
    title = "Campaign Websites Grew Longer and Converged in Length",
    x = NULL,
    y = "Median website length (words)"
  ) +
  theme_social() +
  theme(plot.title = element_text(face = "bold", size = 15, hjust = 0))
save_candidate(p7, "proposal_07_website_length_by_party", 7.2, 4.6)

message("all internal proposal figures written to ", out_dir)
