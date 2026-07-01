#!/usr/bin/env Rscript
# Export the DIME candidate subset needed to link our candidates to DIME.
# Reads the parent project's DIME recipients file, keeps candidate rows + the
# id/covariate columns, writes a compact CSV to scp to the droplet.
#
# Run locally (has the parent repo). Usage:
#   Rscript scripts/export_dime_subset.R [out.csv]

suppressMessages(library(dplyr))
args <- commandArgs(trailingOnly = TRUE)
out <- if (length(args) >= 1) args[1] else "/tmp/dime_cand_subset.csv"
dime_path <- path.expand(
  "~/Documents/GitHub/great-recession-proj/raw_data/dime/dime_recipients_1979_2022.rdata")

e <- new.env()
load(dime_path, envir = e)
obj <- if ("cands" %in% ls(e)) "cands" else ls(e)[1]
cands <- get(obj, envir = e)
cat("loaded", obj, ":", nrow(cands), "x", ncol(cands), "\n")

keep <- c("cycle", "fecyear", "election", "bonica.rid", "bonica.cid",
          "name", "lname", "ffname", "fname", "mname", "title", "suffix",
          "party", "state", "seat", "district", "ico.status",
          "recipient.cfscore", "recipient.cfscore.dyn", "dwnom1", "dwnom2",
          "ICPSR", "NID", "Cand.ID", "FEC.ID")
keep <- intersect(keep, names(cands))

sub <- cands %>%
  filter(.data$recipient.type == "cand", !is.na(.data$Cand.ID), .data$Cand.ID != "") %>%
  select(all_of(keep))
cat("candidate rows with Cand.ID:", nrow(sub), "\n")
cat("seat values:\n"); print(table(sub$seat, useNA = "ifany"))

readr::write_csv(sub, out)
cat("wrote", out, "\n")
