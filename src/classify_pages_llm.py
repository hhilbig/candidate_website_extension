#!/usr/bin/env python3
"""
LLM post-processing for page-type classification.

Re-classifies pages tagged as "other" by the URL heuristic using GPT-5 Nano
on the extracted text. Writes a joinable lookup CSV rather than modifying
scraped CSVs in place.

Usage:
    python -m src.classify_pages_llm --office senate --year 2018
    python -m src.classify_pages_llm --all
    python -m src.classify_pages_llm --dry-run --office senate --year 2018
"""

import argparse
import csv
import logging
import os
import time
from pathlib import Path

import pandas as pd

from .utils import load_config, setup_logging

logger = logging.getLogger(__name__)

VALID_PAGE_TYPES = {
    "homepage", "issues", "biography", "news",
    "endorsements", "constituent_services", "action", "other",
}

CLASSIFICATION_PROMPT = """\
Classify this political candidate's web page into exactly one category.

Categories:
- homepage: Main landing page or site root
- issues: Policy positions, platform, legislative agenda, specific issue pages
- biography: About the candidate, personal background, career history
- news: Press releases, media coverage, blog posts, news articles
- endorsements: Supporter lists, endorsement announcements
- constituent_services: Help for constituents, office locations, casework, resources
- action: Donate, volunteer, events, get involved, sign up
- other: Privacy policy, terms, site infrastructure, or none of the above

URL: {original_url}
First 200 words of page text:
{text_excerpt}

Respond with only the category name, nothing else."""


def _extract_original_url(snap_url: str) -> str:
    """Extract the original URL from a Wayback snapshot URL, without timestamp."""
    import re
    match = re.match(r"https?://web\.archive\.org/web/\d+[^/]*/(.+)", snap_url)
    if match:
        return match.group(1)
    return snap_url


def _first_n_words(text: str, n: int = 200) -> str:
    """Return the first n words of text."""
    words = text.split()
    return " ".join(words[:n])


def _make_url_pattern(original_url: str) -> str:
    """
    Create a URL pattern for matching across snapshots.

    Strips protocol and trailing slash so the same page across different
    timestamps and http/https variants all match.
    """
    url = original_url
    for prefix in ("https://", "http://"):
        if url.startswith(prefix):
            url = url[len(prefix):]
    return url.rstrip("/")


def find_other_pages(snapshots_dir: str, office: str | None = None,
                     year: int | None = None) -> pd.DataFrame:
    """
    Scan scraped CSVs and collect unique URLs classified as 'other'.

    For each unique URL pattern, keeps the row with the longest text
    (best chance of meaningful content for the LLM).

    Returns DataFrame with columns: snap_url_pattern, original_url, text_excerpt, source_file
    """
    base = Path(snapshots_dir)
    if office and year:
        search_dirs = [base / office / str(year)]
    elif office:
        search_dirs = sorted(base.glob(f"{office}/*/"))
    else:
        search_dirs = sorted(base.glob("*/*/"))

    rows = []
    for d in search_dirs:
        if not d.is_dir():
            continue
        for csv_file in sorted(d.glob("*.csv")):
            try:
                df = pd.read_csv(csv_file, dtype=str).fillna("")
            except Exception as e:
                logger.warning(f"Skipping {csv_file}: {e}")
                continue

            if "page_type" not in df.columns or "snap_url" not in df.columns:
                continue

            others = df[df["page_type"] == "other"].copy()
            if others.empty:
                continue

            for _, row in others.iterrows():
                original_url = _extract_original_url(row["snap_url"])
                pattern = _make_url_pattern(original_url)
                text = row.get("text_snap_content", "")
                n_chars = len(text) if text else 0
                rows.append({
                    "snap_url_pattern": pattern,
                    "original_url": original_url,
                    "text": text,
                    "n_chars": n_chars,
                    "source_file": str(csv_file),
                })

    if not rows:
        return pd.DataFrame(columns=["snap_url_pattern", "original_url",
                                      "text_excerpt", "source_file"])

    df_all = pd.DataFrame(rows)

    # Keep the row with the longest text per URL pattern
    df_all = df_all.sort_values("n_chars", ascending=False)
    df_dedup = df_all.drop_duplicates(subset="snap_url_pattern", keep="first")

    df_dedup = df_dedup.copy()
    df_dedup["text_excerpt"] = df_dedup["text"].apply(
        lambda t: _first_n_words(t, 200)
    )

    return df_dedup[["snap_url_pattern", "original_url", "text_excerpt",
                      "source_file"]].reset_index(drop=True)


def load_progress(lookup_path: str) -> set[str]:
    """Load already-classified URL patterns from the lookup CSV."""
    if not os.path.exists(lookup_path):
        return set()
    try:
        df = pd.read_csv(lookup_path, dtype=str)
        return set(df["snap_url_pattern"].tolist())
    except Exception:
        return set()


def classify_with_llm(original_url: str, text_excerpt: str,
                      client, model: str) -> str:
    """
    Send a single page to GPT-5 Nano for classification.

    Returns one of the valid page type strings.
    """
    prompt = CLASSIFICATION_PROMPT.format(
        original_url=original_url,
        text_excerpt=text_excerpt if text_excerpt else "(no text available)",
    )

    response = client.chat.completions.create(
        model=model,
        max_completion_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.choices[0].message.content.strip().lower()

    # Validate response
    if raw in VALID_PAGE_TYPES:
        return raw

    # Try to extract a valid type from the response
    for pt in VALID_PAGE_TYPES:
        if pt in raw:
            return pt

    logger.warning(f"LLM returned invalid type '{raw}' for {original_url}, defaulting to 'other'")
    return "other"


def append_result(lookup_path: str, snap_url_pattern: str, page_type_llm: str):
    """Append a single classification result to the lookup CSV."""
    os.makedirs(os.path.dirname(lookup_path), exist_ok=True)
    write_header = not os.path.exists(lookup_path)

    with open(lookup_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["snap_url_pattern", "page_type_llm"])
        if write_header:
            writer.writeheader()
        writer.writerow({
            "snap_url_pattern": snap_url_pattern,
            "page_type_llm": page_type_llm,
        })


def run_classification(office: str | None, year: int | None,
                       config: dict, dry_run: bool = False):
    """Main classification loop."""
    cls_config = config.get("classification", {})
    model = cls_config.get("llm_model", "gpt-5-nano")
    max_words = cls_config.get("max_text_words", 200)
    delay = cls_config.get("batch_delay_seconds", 0.05)
    env_var = cls_config.get("env_var", "OPENAI_API_KEY")
    snapshots_dir = config.get("output", {}).get("snapshots_dir", "data/snapshots")
    lookup_path = os.path.join(
        config.get("output", {}).get("base_dir", "data"),
        "page_type_llm.csv",
    )

    # Find pages to classify
    logger.info(f"Scanning for 'other' pages in {snapshots_dir} "
                f"(office={office}, year={year})")
    df = find_other_pages(snapshots_dir, office, year)
    logger.info(f"Found {len(df)} unique URL patterns classified as 'other'")

    if df.empty:
        logger.info("Nothing to classify.")
        return

    # Check for already-classified URLs
    done = load_progress(lookup_path)
    remaining = df[~df["snap_url_pattern"].isin(done)]
    logger.info(f"Already classified: {len(done)}, remaining: {len(remaining)}")

    if remaining.empty:
        logger.info("All URLs already classified.")
        return

    if dry_run:
        logger.info("DRY RUN — not calling LLM. Sample URLs:")
        for _, row in remaining.head(10).iterrows():
            excerpt = row["text_excerpt"][:80] + "..." if len(row["text_excerpt"]) > 80 else row["text_excerpt"]
            logger.info(f"  {row['original_url'][:80]}  |  text: {excerpt}")

        # Cost estimate
        avg_input_tokens = 250  # ~200 words + prompt template
        avg_output_tokens = 75  # ~64 reasoning + ~10 completion tokens
        n = len(remaining)
        # GPT-5 Nano pricing: $0.05/1M input, $0.40/1M output (incl. reasoning)
        est_cost = n * (avg_input_tokens * 0.05 / 1_000_000
                        + avg_output_tokens * 0.40 / 1_000_000)
        logger.info(f"Estimated cost: ${est_cost:.2f} for {n} API calls")
        return

    # Initialize OpenAI client
    api_key = os.environ.get(env_var)
    if not api_key:
        logger.error(f"Missing API key. Set {env_var} environment variable.")
        return

    import openai
    client = openai.OpenAI(api_key=api_key)

    # Classify each URL
    classified = 0
    errors = 0
    for idx, row in remaining.iterrows():
        try:
            page_type = classify_with_llm(
                row["original_url"],
                _first_n_words(row["text_excerpt"], max_words),
                client, model,
            )
            append_result(lookup_path, row["snap_url_pattern"], page_type)
            classified += 1

            if classified % 100 == 0:
                logger.info(f"Classified {classified}/{len(remaining)} "
                            f"({errors} errors)")

            time.sleep(delay)

        except Exception as e:
            errors += 1
            logger.error(f"Error classifying {row['original_url'][:80]}: {e}")
            if errors > 50:
                logger.error("Too many errors, stopping.")
                break
            time.sleep(1)  # longer delay after error

    logger.info(f"Done. Classified {classified} URLs, {errors} errors.")
    logger.info(f"Results written to {lookup_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Re-classify 'other' pages using Claude Haiku",
    )
    parser.add_argument("--office", choices=["house", "senate"],
                        help="Limit to one office")
    parser.add_argument("--year", type=int,
                        help="Limit to one election year")
    parser.add_argument("--all", action="store_true",
                        help="Process all offices and years")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be classified without calling LLM")
    parser.add_argument("--config", default="config/config.yaml",
                        help="Path to config file")

    args = parser.parse_args()

    if not args.all and not args.office:
        parser.error("Specify --office (and optionally --year), or --all")

    setup_logging()
    config = load_config(args.config)

    office = None if args.all else args.office
    year = args.year if not args.all else None

    run_classification(office, year, config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
