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
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

BLUE <- "#2f6f9f"
DARK <- "#1d4f77"
GREY <- "grey55"
BAND <- "#2f6f9f"

theme_social <- function(fontsize = 13) {
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

p1 <- ggplot(gaps, aes(year, gap, group = source)) +
  annotate("rect", xmin = 2016.5, xmax = 2017.5,
           ymin = -Inf, ymax = Inf, fill = BAND, alpha = 0.10, color = NA) +
  geom_hline(yintercept = 0, color = "grey70", linewidth = 0.4) +
  geom_line(color = DARK, linewidth = 0.65) +
  geom_point(color = DARK, size = 1.7) +
  facet_wrap(~topic, scales = "free_y", ncol = 2) +
  scale_x_continuous(breaks = seq(2002, 2024, 6)) +
  labs(x = NULL, y = "Democratic minus Republican attention (pp)") +
  theme_social()
save_candidate(p1, "proposal_01_long_run_partisan_gaps", 7.8, 5.2)

a24 <- read_csv(file.path(data_dir, "agenda_2024.csv"), show_col_types = FALSE) |>
  mutate(
    gap = democrat - republican,
    topic = fct_reorder(topic, gap)
  )

p2 <- ggplot(a24, aes(y = topic)) +
  geom_segment(aes(x = democrat, xend = republican, yend = topic),
               color = "grey85", linewidth = 0.9) +
  geom_point(aes(x = democrat), color = DARK, size = 2.6) +
  geom_point(aes(x = republican), color = GREY, size = 2.6) +
  geom_text(data = filter(a24, topic == "Welfare State"),
            aes(x = democrat, label = "Democrats"),
            color = DARK, nudge_y = 0.28, hjust = 1.08,
            size = 3.0, fontface = "bold") +
  geom_text(data = filter(a24, topic == "Welfare State"),
            aes(x = republican, label = "Republicans"),
            color = GREY, nudge_y = 0.28, size = 3.0) +
  labs(title = "What the Parties Emphasized in 2024",
       x = "Share of topic attention (%)", y = NULL) +
  theme_social() +
  theme(plot.title = element_text(face = "bold", size = 15, hjust = 0))
save_candidate(p2, "proposal_02_2024_agenda", 7.2, 4.6)

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

evolution <- read_csv(file.path(data_dir, "website_evolution.csv"),
                      show_col_types = FALSE) |>
  mutate(
    measure = factor(measure, c("Website length", "Lexical diversity"))
  ) |>
  group_by(measure) |>
  mutate(change = 100 * (value / value[year == 2002] - 1)) |>
  ungroup()

evolution_labels <- evolution |>
  filter(year == 2024) |>
  mutate(label = if_else(
    measure == "Website length",
    sprintf("+%.0f%% (%.0f words)", change, value),
    sprintf("%.1f%%", change)
  ))

p7 <- ggplot(evolution, aes(year, change, group = source, color = source)) +
  annotate("rect", xmin = 2016.5, xmax = 2017.5,
           ymin = -Inf, ymax = Inf, fill = BAND, alpha = 0.10, color = NA) +
  geom_hline(yintercept = 0, color = "grey75", linewidth = 0.4) +
  geom_line(linewidth = 0.65) +
  geom_point(size = 1.7) +
  geom_text(data = evolution_labels, aes(label = label),
            color = "grey20", hjust = 1.08, vjust = -0.7, size = 3.0) +
  facet_wrap(~measure, ncol = 2) +
  scale_color_manual(values = c("Di Tella et al." = GREY,
                                "This release" = DARK)) +
  scale_x_continuous(breaks = seq(2002, 2024, 6),
                     limits = c(2002, 2025)) +
  scale_y_continuous(breaks = c(0, 50, 100, 150),
                     limits = c(-10, 170)) +
  labs(
    title = "Campaign Websites Became Longer, but Not More Diverse",
    x = NULL,
    y = "Change from 2002 median (%)"
  ) +
  theme_social() +
  theme(plot.title = element_text(face = "bold", size = 15, hjust = 0))
save_candidate(p7, "proposal_07_length_and_lexical_diversity", 7.8, 4.6)

message("all internal proposal figures written to ", out_dir)
