#!/usr/bin/env python3
"""Reproduce ICPSR 226001's candidate-level coding, and apply it to our corpus.

ICPSR 226001 (Di Tella, Kotti, Le Pennec, Pons) ships *coded variables*, not
text. To let our extension stack onto theirs with comparable columns we have to
compute the same variables the same way. This script does two things:

  --validate  reproduce ICPSR's own published values from their own text, so the
              coding is verified before we apply it to anything
  --apply     compute the verified columns for our corpus

Design rule: this script NEVER modifies or replaces our existing columns. It
writes a separate, join-able table of ICPSR-compatible columns, all prefixed
`icpsr_`. Where our own measure differs from theirs (e.g. our candidate-year
panel collapses to the longest-text snapshot, theirs averages over snapshots),
both are kept side by side and the reader picks.

Verified findings (see quality_reports/icpsr_variable_extension_2026-08-12.md):

  * Aggregation rule (EXACT, 13,020/13,020 rows, every year x stage):
        drop pages with n_char == 0
        -> mean over pages within a snapshot `date`
        -> mean over snapshot dates
    The grain is candidate x state x district x year x stage x data_source.
    A candidate-year-stage can appear twice (primary and general_wayback),
    distinguished in the raw data only by name casing.

  * n_char  == len(text_snap_content)                    (100.00% exact)
  * n_words == len(re.findall(r'\\w+', text.lower()))     (100.00% exact)
  * TTR / MATTR (window 200) use a different tokenizer from n_words, also given
    in 4_complexity.R. corr 0.9997 / 0.9995, median ratio 1.0000. Not bit-exact
    (37%) because quanteda tokenises on ICU word boundaries; shipped `_approx`.
  * entropy = term-weighted mean of PER-WORD Google Books entropy from
    ngrams_en_2008.csv, not a document statistic. corr 0.9978, ratio 0.9997.
  * topics = a supervised SVM trained on Manifesto Project quasi-sentences,
    not a topic model. MAE 0.0054 against their published values, versus a
    0.0296 predict-the-mean baseline; top-1 topic agreement 88%.

  Deliberately NOT shipped:
  * n_tags / n_clean_tags -- HTML-structure counts fixed at scrape time; the
    archived HTML is gone and the cleaned text has `#+#` stripped.
  * entropy_missing -- corr 0.952, below the 0.99 bar; a diagnostic only.
  * subordinates -- defined as POS tag `IN` / n_words, but ICPSR used openNLP's
    Maxent tagger. nltk's perceptron tagger gives corr 0.975, so this would be a
    related measure rather than theirs.

Usage:
    python scripts/icpsr_replicate_coding.py --validate
    python scripts/icpsr_replicate_coding.py --fetch-manifesto     # needs API key
    python scripts/icpsr_replicate_coding.py --apply --topics
"""
from __future__ import annotations

import argparse
import math
import re
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ICPSR_DIR = Path.home() / (
    "Library/CloudStorage/Dropbox/Research/19_Great_Recession"
    "/data/candidate_websites/226001-V1")

# ICPSR's word count: `n_words <- function(t) str_count(t, '\\w+')` in
# 4_complexity.R. Verified at 100% against websites_clean.n_words.
TOKEN_RE = re.compile(r"\w+")

# TTR/MATTR use a DIFFERENT tokenizer, also from 4_complexity.R:
#   str_remove_all(fixed("'s")) -> tokens(remove_punct, remove_symbols,
#   remove_numbers, remove_url) -> dfm(tolower=TRUE) -> dfm_select("^[a-zA-Z]+$")
# quanteda keeps internal apostrophes/hyphens inside a token, and the
# ^[a-zA-Z]+$ filter then drops those tokens; splitting them instead (a bare
# [A-Za-z]+ scan) keeps both halves and inflates the type count.
URL_RE = re.compile(r"(https?://\S+|www\.\S+)", re.I)
WORDLIKE_RE = re.compile(r"[A-Za-z]+(?:['\-][A-Za-z]+)*")

# Variables carried directly on ICPSR's per-page rows
PAGE_VARS = ["n_char", "n_words", "n_tags", "n_clean_tags"]


# --------------------------------------------------------------------------
# core coding
# --------------------------------------------------------------------------
def tokenize(text: str) -> list[str]:
    """ICPSR's word-count tokenizer. Verified exactly against their n_words."""
    return TOKEN_RE.findall(text.lower())


def lexdiv_tokens(text: str) -> list[str]:
    """ICPSR's TTR/MATTR tokenizer (see WORDLIKE_RE note above)."""
    text = URL_RE.sub(" ", text.replace("'s", ""))
    return [t.lower() for t in WORDLIKE_RE.findall(text) if t.isalpha()]


def _mattr(toks: list[str], win: int) -> float:
    """Moving-average type-token ratio, O(n) via a rolling counter."""
    n = len(toks)
    if n == 0:
        return float("nan")
    if n < win:
        return len(set(toks)) / n
    cnt = Counter(toks[:win])
    types = len(cnt)
    acc = types
    for i in range(win, n):
        out, inc = toks[i - win], toks[i]
        cnt[out] -= 1
        if cnt[out] == 0:
            del cnt[out]
            types -= 1
        if inc not in cnt:
            types += 1
        cnt[inc] += 1
        acc += types
    return acc / (n - win + 1) / win


def load_ngrams(path: Path) -> tuple[dict, set]:
    """Google Books word entropies, the input to ICPSR's `entropy`.

    Returns (token -> entropy, tokens that have a book_frequency).
    """
    ng = pd.read_csv(path).drop_duplicates("token", keep="first")
    return (dict(zip(ng.token, ng.entropy)),
            set(ng.token[ng.book_frequency.notna()]))


# --------------------------------------------------------------------------
# topics (7_topics.py)
# --------------------------------------------------------------------------
# Their "topic model" is a supervised classifier, not a topic model:
# TfidfVectorizer + SVC(rbf, C=20, probability=True, random_state=2163) trained
# on Manifesto Project quasi-sentences, then predict_proba. `topic_words.csv` is
# that classifier applied to each vocabulary word, and `tfidf_weight` is the
# vocabulary index, not a weight.
#
# The training corpus is redistribution-restricted (ICPSR stripped it from their
# deposit for the same reason), so we do NOT commit it. Fetch it yourself with
# --fetch-manifesto and a Manifesto Project API key.
MANIFESTO_API = "https://manifesto-project.wzb.eu/api/v1/"
MANIFESTO_VERSION = "2021-1"     # identified by vocabulary match: 98.8% of their
MANIFESTO_CORE = "MPDS2021a"     # 7,668 terms; 2020-2 and earlier are far smaller
SVM_SEED = 2163


def fetch_manifesto(out_path: Path, key_path: Path,
                    version: str = MANIFESTO_VERSION,
                    core_key: str = MANIFESTO_CORE) -> None:
    """Download the US Manifesto quasi-sentences used to train the classifier."""
    import json
    import urllib.parse
    import urllib.request

    api_key = key_path.read_text().strip()

    def call(fn, **params):
        params["api_key"] = api_key
        url = MANIFESTO_API + fn + "?" + urllib.parse.urlencode(params, doseq=True)
        with urllib.request.urlopen(url, timeout=180) as r:
            return json.loads(r.read().decode("utf-8"))

    core = call("get_core", key=core_key)
    cols, rows = core[0], core[1:]
    ip, idt = cols.index("party"), cols.index("date")
    keys = [f"{int(r[ip])}_{int(r[idt])}" for r in rows
            if str(int(r[ip])).startswith("61")]          # 61xxx = United States
    meta = call("metadata", **{"keys[]": keys, "version": version})
    mids = [it["manifesto_id"] for it in meta["items"] if "manifesto_id" in it]
    out = []
    for i in range(0, len(mids), 10):
        res = call("texts_and_annotations",
                   **{"keys[]": mids[i:i + 10], "version": version})
        for doc in res.get("items", []):
            for it in doc.get("items", []):
                t, c = it.get("text"), it.get("cmp_code")
                if t and c is not None:
                    out.append({"text": t, "code": str(c)})
        print(f"  {min(i+10, len(mids))}/{len(mids)} documents, "
              f"{len(out):,} sentences", flush=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out))
    print(f"wrote {out_path} ({len(out):,} quasi-sentences)")


def _manifesto_cleaner():
    """ICPSR's training-text preprocessing, memoised on the raw word."""
    import string
    from nltk.corpus import stopwords
    from nltk.stem import SnowballStemmer

    stop = set(stopwords.words("english"))
    stemmer = SnowballStemmer(language="english")
    punct = str.maketrans("", "", string.punctuation)
    cache: dict[str, str] = {}

    def clean(t: str) -> str:
        out = []
        for w in t.split():
            if w in stop:
                continue
            s = cache.get(w)
            if s is None:
                s = cache[w] = stemmer.stem(w.lower().translate(punct))
            out.append(s)
        return " ".join(out)

    return clean


def train_topic_model(manifesto_path: Path, mapping_path: Path):
    """Rebuild ICPSR's classifier. Returns (pipeline, cleaner, class names)."""
    import json
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.pipeline import Pipeline
    from sklearn.svm import SVC

    df = pd.DataFrame(json.loads(manifesto_path.read_text()))

    def domain(c):
        c = c.strip()
        return int(c[0]) if c[:1].isdigit() and c[0] in "1234567" else np.nan
    df = df[df.code.map(domain).notna()].reset_index(drop=True)
    df["code"] = df.code.str.replace(r"\.0$", "", regex=True)
    cats = pd.read_csv(mapping_path, dtype={"code": object})[
        ["code", "domain_name_consolidated"]]
    df = df.merge(cats, how="left", on="code")
    df["domain_name_consolidated"] = df.domain_name_consolidated.fillna("Other")
    y = (df.domain_name_consolidated.rank(method="dense").astype(int) - 1).to_numpy()
    names = df.domain_name_consolidated.drop_duplicates().sort_values().tolist()

    clean = _manifesto_cleaner()
    X = df.text.apply(clean).to_numpy()
    pipe = Pipeline([
        ("vectoriser", TfidfVectorizer()),
        ("classifier", SVC(probability=True, random_state=SVM_SEED, C=20,
                           gamma="scale", kernel="rbf")),
    ])
    print(f"training topic classifier on {len(df):,} quasi-sentences, "
          f"{len(names)} classes ...", flush=True)
    pipe.fit(X, y)
    return pipe, clean, names


def page_measures(text: str, mattr_window: int,
                  ngrams: tuple[dict, set] | None = None) -> dict:
    """Text-derived measures for a single page.

    n_words and the lexical-diversity measures use different tokenizers,
    matching 4_complexity.R.
    """
    out = {"n_char": len(text), "n_words": len(tokenize(text))}
    lex = lexdiv_tokens(text)
    n = len(lex)
    if n == 0:
        out["ttr"] = out["mattr"] = np.nan
        if ngrams is not None:
            out["entropy"] = out["entropy_missing"] = np.nan
        return out
    counts = Counter(lex)
    out["ttr"] = len(counts) / n
    # ICPSR gate their MATTR on `rowSums(dfm) > window` and fall back to TTR
    # otherwise: `MATTR <- ifelse(is.na(MATTR), TTR, MATTR)`.
    out["mattr"] = _mattr(lex, mattr_window) if n > mattr_window else out["ttr"]

    if ngrams is not None:
        ent, has_bookfreq = ngrams
        num = den = 0.0
        missing = 0
        for tok, k in counts.items():
            e = ent.get(tok)
            if e is not None and e == e:      # skip NaN
                num += k * e
                den += k
            if tok not in has_bookfreq:
                missing += k
        out["entropy"] = num / den if den > 0 else np.nan
        out["entropy_missing"] = missing / n
    return out


def icpsr_aggregate(pages: pd.DataFrame, value_cols: list[str],
                    group_col: str = "ck",
                    date_col: str = "date") -> pd.DataFrame:
    """ICPSR's snapshot -> candidate aggregation.

    Drop empty pages, mean over pages within a snapshot date, mean over dates.
    Verified exact on all 13,020 published rows.
    """
    valid = pages[pages["n_char"] > 0]
    per_snap = valid.groupby([group_col, date_col])[value_cols].mean()
    return per_snap.groupby(group_col).mean()


def icpsr_key(df: pd.DataFrame) -> pd.Series:
    """ICPSR's candidate grain, including data_source.

    The same candidate-year-stage can appear once as `primary` and once as
    `general_wayback`; in the raw files they are distinguished only by the
    casing of the name, so data_source must be part of the key.
    """
    return (df["candidate"].str.upper().str.strip() + "|"
            + df["state"].str.lower().str.strip() + "|"
            + df["district"].astype(str) + "|"
            + df["year"].astype(str) + "|"
            + df["stage"].astype(str) + "|"
            + df["data_source"].str.strip())


# --------------------------------------------------------------------------
# validate
# --------------------------------------------------------------------------
def run_validate(mattr_window: int, sample: int, ngrams_path: Path) -> int:
    truth = pd.read_csv(ICPSR_DIR / "candidates_complexity.csv")
    w = pq.read_table(ICPSR_DIR / "websites_clean.parquet").to_pandas()
    w["ck"] = icpsr_key(w)
    truth["ck"] = icpsr_key(truth)

    print(f"websites_clean pages : {len(w):,}")
    print(f"complexity rows      : {len(truth):,}")
    print(f"key overlap          : {truth.ck.isin(set(w.ck)).sum():,}"
          f" / {len(truth):,}\n")

    print("=== aggregation rule, on ICPSR's own per-page values ===")
    rep = icpsr_aggregate(w, PAGE_VARS)
    m = truth.set_index("ck")[PAGE_VARS].join(rep, rsuffix="_rep")
    ok = True
    for v in PAGE_VARS:
        rate = float(((m[v] - m[v + "_rep"]).abs() < 1e-6).mean())
        ok &= rate >= 0.99
        print(f"  {v:<14} {100*rate:6.2f}% exact")

    ngrams = load_ngrams(ngrams_path) if ngrams_path.exists() else None
    if ngrams is None:
        print(f"NOTE: {ngrams_path} absent; entropy not validated\n")

    print("\n=== per-page measures recomputed from text (exact reproduction) ===")
    s = w[w.n_char > 0]
    if sample and sample < len(s):
        s = s.sample(sample, random_state=20260812)
    meas = pd.DataFrame([page_measures(t, mattr_window)
                         for t in s.text_snap_content], index=s.index)
    for v in ("n_char", "n_words"):
        rate = float((meas[v] == s[v]).mean())
        ok &= rate >= 0.99
        print(f"  {v:<14} {100*rate:6.2f}% exact  (n={len(s):,})")

    # TTR/MATTR are candidate-level aggregates, so they must be validated on
    # COMPLETE candidate-years. Sampling pages would give each candidate-year a
    # random partial page set and understate agreement.
    print("\n=== TTR / MATTR (approximate; agreement with published values) ===")
    rng = np.random.default_rng(20260812)
    n_cand = max(1, min(250, truth.ck.nunique()))
    cks = rng.choice(truth.ck.unique(), size=n_cand, replace=False)
    full = w[w.ck.isin(set(cks)) & (w.n_char > 0)].copy()
    fm = pd.DataFrame([page_measures(t, mattr_window, ngrams)
                       for t in full.text_snap_content], index=full.index)
    cols = ["ttr", "mattr"] + (["entropy", "entropy_missing"] if ngrams else [])
    for c_ in cols:
        full[c_] = fm[c_]
    agg = icpsr_aggregate(full, cols)
    pairs = [("TTR", "ttr"), ("MATTR", "mattr")]
    if ngrams:
        pairs += [("entropy", "entropy"), ("entropy_missing", "entropy_missing")]
    j = truth.set_index("ck").loc[cks][[p[0] for p in pairs]].join(
        agg, rsuffix="_rep").dropna()
    for tcol, rcol in [(a, b if b != a else b + "_rep") for a, b in pairs]:
        corr = float(np.corrcoef(j[tcol], j[rcol])[0, 1])
        ratio = float((j[tcol] / j[rcol]).median())
        good = corr >= 0.99 and abs(ratio - 1) <= 0.01
        # entropy_missing is reported as a diagnostic only. It is the most
        # tokenizer-sensitive measure (corr ~0.95) and is not shipped, so it
        # does not gate the verdict.
        if tcol != "entropy_missing":
            ok &= good
        verdict = ("not shipped" if tcol == "entropy_missing"
                   else ("ok" if good else "FAIL"))
        print(f"  {tcol:<16} corr {corr:6.4f}  median ratio {ratio:6.4f}  "
              f"({len(j):,} cand-years, {len(full):,} pages)  {verdict}")

    print(f"\nVALIDATE {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


# --------------------------------------------------------------------------
# apply
# --------------------------------------------------------------------------
def run_apply(corpus: Path, out: Path, mattr_window: int,
              ngrams_path: Path, topic_model=None) -> int:
    ngrams = load_ngrams(ngrams_path) if ngrams_path.exists() else None
    if ngrams is None:
        print(f"NOTE: {ngrams_path} absent; entropy columns will be skipped")
    pf = pq.ParquetFile(corpus)
    print(f"corpus: {corpus}  ({pf.metadata.num_rows:,} rows)")

    cols = ["candidate_icpsr", "name_key", "cand_id", "state", "district_id",
            "office", "year", "stage", "party", "data_source", "date",
            "n_char", "text_snap_content"]
    have = set(pf.schema_arrow.names)
    use = [c for c in cols if c in have]
    missing = [c for c in cols if c not in have]
    if missing:
        print(f"  note: absent from corpus, skipped: {missing}")

    topics = None
    if topic_model is not None:
        pipe, clean, class_names = topic_model
        topic_keys: list[str] = []
        topic_probs: list[np.ndarray] = []
        batch_keys: list[str] = []
        batch_docs: list[str] = []

        def flush_topics():
            if not batch_docs:
                return
            topic_probs.append(pipe.predict_proba(batch_docs))
            topic_keys.extend(batch_keys)
            batch_keys.clear()
            batch_docs.clear()
            print(f"    topics scored: {len(topic_keys):,}", flush=True)

    frames = []
    for i in range(pf.num_row_groups):
        g = pf.read_row_group(i, columns=use).to_pandas()
        g = g[g.n_char > 0]
        if g.empty:
            continue
        if topic_model is not None:
            # Topics use ICPSR's own document rule from 7_topics.py: all page
            # text for the candidate-year joined by a space. Note this is a
            # DIFFERENT aggregation from the complexity two-level mean.
            kc = [c for c in ("candidate_icpsr", "state", "office", "year")
                  if c in g.columns]
            batch_keys.append("|".join(str(g[c].iloc[0]) for c in kc))
            batch_docs.append(clean(" ".join(g.text_snap_content)))
            if len(batch_docs) >= 400:
                flush_topics()
        meas = pd.DataFrame([page_measures(t, mattr_window, ngrams)
                             for t in g.text_snap_content], index=g.index)
        # The corpus already carries n_char/n_words computed by our own
        # pipeline. Drop those copies here and use the recomputed ones, so the
        # aggregate is built from ICPSR's definitions throughout. (Our columns
        # are untouched in the corpus itself; this only affects this output.)
        g = g.drop(columns=[c for c in ("text_snap_content", "n_char", "n_words",
                                        "n_tags", "n_clean_tags")
                            if c in g.columns])
        # Our `date` is a Wayback YYYYMMDDHHMMSS timestamp; ICPSR's snapshot
        # grain is the DAY. Truncate so "mean over snapshot dates" means the
        # same thing on both sides.
        g["snap_day"] = g["date"].astype(str).str.slice(0, 8)
        g = pd.concat([g, meas], axis=1)
        frames.append(g)
        if (i + 1) % 1000 == 0 or i + 1 == pf.num_row_groups:
            print(f"  row group {i+1}/{pf.num_row_groups}", flush=True)

    if topic_model is not None:
        flush_topics()
        topics = pd.DataFrame(np.vstack(topic_probs),
                              columns=[f"icpsr_topic_{c}" for c in class_names],
                              index=topic_keys)

    pages = pd.concat(frames, ignore_index=True)
    del frames
    print(f"\ntotal non-empty pages: {len(pages):,}")

    keycols = [c for c in ("candidate_icpsr", "state", "office", "year")
               if c in pages.columns]
    pages["ck"] = pages[keycols].astype(str).agg("|".join, axis=1)

    vals = ["n_char", "n_words", "ttr", "mattr"]
    if ngrams:
        vals += ["entropy"]   # entropy_missing not shipped (corr ~0.95)
    agg = icpsr_aggregate(pages, vals, date_col="snap_day")
    agg = agg.rename(columns={"n_char": "icpsr_n_char",
                              "n_words": "icpsr_n_words",
                              "ttr": "icpsr_ttr_approx",
                              "mattr": "icpsr_mattr_approx",
                              "entropy": "icpsr_entropy_approx"})
    carry = [c for c in ("cand_id", "name_key", "district_id", "party", "stage",
                         "data_source") if c in pages.columns]
    meta = pages.drop_duplicates("ck").set_index("ck")[keycols + carry]
    n_snap = (pages.groupby("ck").snap_day.nunique()
                   .rename("icpsr_n_valid_snap"))
    n_page = pages.groupby("ck").size().rename("icpsr_n_valid_pages")
    res = meta.join(agg).join(n_snap).join(n_page)
    if topics is not None:
        res = res.join(topics)
        bad = int((res[topics.columns].sum(axis=1).sub(1).abs() > 1e-9).sum())
        print(f"topic rows failing the sum-to-1 check: {bad}")
    res = res.reset_index(drop=True)

    dup = res.duplicated(subset=keycols).sum()
    print(f"key {keycols}: {len(res):,} rows, {dup} duplicates")
    for c in [c for c in res.columns if c.startswith("icpsr_")]:
        print(f"  {c:<22} missing {res[c].isna().sum():>5}  "
              f"median {res[c].median():.4f}")

    out.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(out, index=False)
    print(f"\nwrote {out}  ({len(res):,} candidate-years, {len(res.columns)} cols)")
    print("columns:", ", ".join(res.columns))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--corpus", type=Path,
                    default=Path("data/deliverable/raw_corpus_icpsr.parquet"))
    ap.add_argument("--out", type=Path,
                    default=Path("data/deliverable/panel_icpsr_compat.csv"))
    ap.add_argument("--mattr-window", type=int, default=200,
                    help="verified optimum against ICPSR (searched 10..500)")
    ap.add_argument("--fetch-manifesto", action="store_true",
                    help="download the Manifesto training corpus (needs an API key)")
    ap.add_argument("--manifesto", type=Path,
                    default=Path("data/external/us_manifesto_2021-1.json"))
    ap.add_argument("--manifesto-key", type=Path,
                    default=Path.home() / ".manifesto_api_key")
    ap.add_argument("--mapping", type=Path,
                    default=Path("data/external/sub_topics_mapping.csv"))
    ap.add_argument("--topics", action="store_true",
                    help="also compute the 31 topic columns (slow)")
    ap.add_argument("--ngrams", type=Path,
                    default=Path("data/external/ngrams_en_2008.csv"),
                    help="Google Books word entropies (ICPSR external/ngrams)")
    ap.add_argument("--sample", type=int, default=200000,
                    help="pages sampled for the per-page validation (0 = all)")
    a = ap.parse_args()
    if a.fetch_manifesto:
        fetch_manifesto(a.manifesto, a.manifesto_key)
        return 0
    if not (a.validate or a.apply):
        ap.error("choose --validate and/or --apply")
    rc = 0
    if a.validate:
        rc |= run_validate(a.mattr_window, a.sample, a.ngrams)
    if a.apply:
        tm = None
        if a.topics:
            if not a.manifesto.exists():
                ap.error(f"{a.manifesto} absent; run --fetch-manifesto first")
            tm = train_topic_model(a.manifesto, a.mapping)
        rc |= run_apply(a.corpus, a.out, a.mattr_window, a.ngrams, tm)
    return rc


if __name__ == "__main__":
    sys.exit(main())
