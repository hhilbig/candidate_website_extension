# Social-media figures for the mayoral release: coverage by year, and website page types by period.
# Inputs come from the release (computed in quality_reports/social_figure_candidates/mayoral/data/).
set.seed(20261023)
library(ggplot2)
library(dplyr)
library(readr)

CLARA_BLUE <- "#2f6f9f"
CLARA_DARK <- "#1d4f77"
CLARA_GREY <- "grey55"

theme_clara <- function(base_size = 13) {
  theme_bw(base_size = base_size) +
    theme(
      panel.grid = element_blank(),
      panel.border = element_rect(color = "grey70"),
      axis.ticks = element_blank(),
      legend.position = "none",
      plot.margin = margin(8, 14, 8, 8)
    )
}

save_clara <- function(plot, path_stem, width, height) {
  ggsave(paste0(path_stem, ".pdf"), plot, width = width, height = height, device = cairo_pdf)
  ggsave(paste0(path_stem, ".png"), plot, width = width, height = height, dpi = 450, bg = "white")
}

dir <- "quality_reports/social_figure_candidates/mayoral"

# Figure 1: share of finalists with usable website text, winners and runners-up, by election year.
cov <- read_csv(file.path(dir, "data/coverage_by_year_role.csv"), show_col_types = FALSE) |>
  mutate(role = if_else(official_result_role == "winner", "Winners", "Runners-up"))
ends <- cov |> filter(year == max(year))
p1 <- ggplot(cov, aes(year, share, group = role)) +
  geom_line(aes(color = role), linewidth = 0.7) +
  geom_point(aes(color = role), size = 1.8) +
  geom_text(data = ends, aes(label = role, color = role), hjust = -0.15, size = 3.8) +
  scale_color_manual(values = c(Winners = CLARA_DARK, `Runners-up` = CLARA_GREY)) +
  scale_x_continuous(breaks = seq(2010, 2026, 2), expand = expansion(mult = c(0.02, 0.17))) +
  scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, 25)) +
  coord_cartesian(clip = "off") +
  labs(x = NULL, y = "Finalists with website text (%)") +
  theme_clara()
save_clara(p1, file.path(dir, "figures/mayoral_coverage_by_year"), 7.5, 4.2)

# Figure 2: share of finalists with text whose website has a page of each type, by four-year period.
pt <- read_csv(file.path(dir, "data/page_types_by_period.csv"), show_col_types = FALSE) |>
  mutate(x = as.integer(factor(period)))
# Spread endpoint labels that would overlap: at least 4 points apart, in plotted order.
lab <- pt |> filter(x == max(x)) |> arrange(share) |> mutate(label_y = share)
for (i in seq_len(nrow(lab))[-1]) lab$label_y[i] <- max(lab$share[i], lab$label_y[i - 1] + 4)
p2 <- ggplot(pt, aes(x, share, group = label)) +
  geom_line(color = CLARA_DARK, linewidth = 0.7) +
  geom_point(color = CLARA_DARK, size = 1.9) +
  geom_text(data = lab, aes(y = label_y, label = label), hjust = -0.12, size = 3.6, color = "grey20") +
  scale_x_continuous(breaks = 1:4, labels = levels(factor(pt$period)), expand = expansion(mult = c(0.04, 0.42))) +
  scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, 25)) +
  coord_cartesian(clip = "off") +
  labs(x = NULL, y = "Finalists whose website has this page (%)") +
  theme_clara()
save_clara(p2, file.path(dir, "figures/mayoral_page_types"), 7.5, 4.6)
