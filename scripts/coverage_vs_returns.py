#!/usr/bin/env python3
"""Coverage measured against the ballot, for both datasets on one scale.

The roster-based coverage figure could only describe this release, because the
roster is ours. This one uses a denominator that exists independently of either
collection: every Democratic and Republican candidate who appeared on a general
election ballot, from the MIT Election Data and Science Lab returns. Both
collections are then matched into it by name, so ICPSR 226001's House years and
this release's years sit on the same axis.

Two shares per office-year:

  pct_cand   captured candidates / candidates on the ballot
  pct_votes  votes cast for captured candidates / votes cast for all major-party
             candidates. A missing candidate who drew 200,000 votes counts more
             than one who drew 8,000.

Sources
  data/external/1976-2024-house.tab          MIT, House, doi:10.7910/DVN/IG0UN2
  data/external/1976-2024-senate-state.csv   MIT, Senate, doi:10.7910/DVN/PEJ5QU
  ICPSR candidates_complexity.csv            stage 2, general_wayback = theirs
  data/deliverable/panel_icpsr_compat.csv    ours

Usage: python scripts/coverage_vs_returns.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
ICPSR = Path.home() / ("Library/CloudStorage/Dropbox/Research/19_Great_Recession"
                       "/data/candidate_websites/226001-V1")
OUT = REPO / "quality_reports/figures/data"

SUFFIX = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "dds", "esq"}
STATE_PO = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "district of columbia": "DC", "florida": "FL",
    "georgia": "GA", "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY",
    "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}


def name_parts(raw: str, comma_form: bool) -> tuple[str, str]:
    """Return (last, first) lowercased, suffixes and punctuation removed.

    comma_form is "Last, First Middle"; otherwise "First Middle Last".
    """
    s = re.sub(r'"[^"]*"', " ", str(raw))          # drop "Nick" names
    s = re.sub(r"[^A-Za-z, ]", " ", s).lower()
    if comma_form and "," in s:
        last, _, first = s.partition(",")
        lt = [w for w in last.split() if w not in SUFFIX]
        ft = [w for w in first.split() if w not in SUFFIX]
    else:
        toks = [w for w in s.replace(",", " ").split() if w not in SUFFIX]
        if not toks:
            return "", ""
        lt, ft = [toks[-1]], toks[:-1]
    return ("".join(lt), (ft[0] if ft else ""))


def last_variants(raw: str) -> list[tuple[str, str]]:
    """Plausible (last, first) splits for a "First Middle Last" ballot name.

    The ballot writes "DEBBIE WASSERMAN SCHULTZ" and "ALEXANDRIA OCASIO-CORTEZ";
    the collections write "Wasserman Schultz, Debbie". Taking only the final
    token as the surname silently drops every multi-word surname, so try the
    last one, two and three tokens and let the caller accept any of them.
    """
    s = re.sub(r'"[^"]*"', " ", str(raw))
    s = re.sub(r"[^A-Za-z ]", " ", s).lower()
    toks = [w for w in s.split() if w not in SUFFIX]
    if not toks:
        return [("", "")]
    out = []
    for k in (1, 2, 3):
        if len(toks) > k:
            out.append(("".join(toks[-k:]), toks[0]))
    if not out:                                   # single-token name
        out.append(("".join(toks), ""))
    return out


def keys(df: pd.DataFrame, name: str, comma: bool) -> pd.DataFrame:
    """Add last/first/initial columns used for the ballot match.

    Collection rows (comma form) get one surname. Ballot rows also get
    `lasts`, every plausible surname split, because their name format hides
    where the surname begins.
    """
    parts = [name_parts(v, comma) for v in df[name]]
    df = df.copy()
    df["last"] = [p[0] for p in parts]
    df["first"] = [p[1] for p in parts]
    df["fi"] = df["first"].str[:1]
    if not comma:
        df["lasts"] = [last_variants(v) for v in df[name]]
    return df


def ballot() -> pd.DataFrame:
    """Every D/R general-election candidate, with votes, House and Senate."""
    out = []
    h = pd.read_csv(REPO / "data/external/1976-2024-house.tab",
                    low_memory=False, encoding="latin-1")
    h = h[(h.stage.str.upper() == "GEN") & (~h.writein.fillna(False))
          & h.party.isin(["DEMOCRAT", "REPUBLICAN"])
          & h.year.between(2002, 2024)]
    h = h.assign(office="house", district=h.district.fillna(0).astype(int))
    out.append(h[["year", "office", "state_po", "district", "candidate",
                  "party", "candidatevotes"]])

    s = pd.read_csv(REPO / "data/external/1976-2024-senate-state.csv",
                    low_memory=False, encoding="latin-1")
    s = s[(s.stage.str.upper() == "GEN") & (~s.writein.fillna(False))
          & s.party_simplified.isin(["DEMOCRAT", "REPUBLICAN"])
          & s.year.between(2002, 2024)]
    s = s.assign(office="senate", district=0, party=s.party_simplified)
    out.append(s[["year", "office", "state_po", "district", "candidate",
                  "party", "candidatevotes"]])

    b = pd.concat(out, ignore_index=True)
    # MIT splits a candidate across voting modes and fusion lines; one row each.
    b = (b.groupby(["year", "office", "state_po", "district", "candidate",
                    "party"], as_index=False).candidatevotes.sum())
    return keys(b, "candidate", comma=False)


def theirs() -> pd.DataFrame:
    t = pd.read_csv(ICPSR / "candidates_complexity.csv")
    t = t[(t.stage == 2) & (t.data_source == "general_wayback")
          & t.year.between(2002, 2016)].copy()
    t["state_po"] = t.state.str.lower().map(STATE_PO)
    t["office"] = "house"
    t["district"] = t.district.fillna(0).astype(int)
    return keys(t, "candidate", comma=True)


def ours() -> pd.DataFrame:
    p = pd.read_csv(REPO / "data/deliverable/panel_icpsr_compat.csv",
                    low_memory=False,
                    usecols=["candidate_icpsr", "state", "office", "year",
                             "district_id", "party"])
    d = p.district_id.astype(str).str.extract(r"(\d+)$")[0]
    p["district"] = pd.to_numeric(d, errors="coerce").fillna(0).astype(int)
    p.loc[p.office == "senate", "district"] = 0
    p = p.rename(columns={"state": "state_po"})
    return keys(p, "candidate_icpsr", comma=True)


def match(b: pd.DataFrame, c: pd.DataFrame) -> pd.Series:
    """True for each ballot row whose candidate appears in collection c.

    Match on (year, office, state, district, last name), then require the first
    names to be compatible: equal, one a prefix of the other (Chris/Christopher),
    same initial, or -- when exactly one person of that surname is in the race on
    each side -- nothing further. That last clause is what catches the ballot's
    "BOB CASEY" against the collection's "Casey, Robert", where even the initial
    disagrees. Requiring uniqueness on both sides keeps it from merging two
    different people who happen to share a surname in one district.
    """
    return pd.Series(_hits(b, c), index=b.index)


def _compatible(g: str, f: str) -> bool:
    """Two first names that plausibly belong to the same person."""
    return (g == f or (bool(g) and bool(f)
                       and (g.startswith(f) or f.startswith(g)))
            or (bool(g[:1]) and g[:1] == f[:1]))


def _hits(b: pd.DataFrame, c: pd.DataFrame) -> list[bool]:
    rc = ["year", "office", "state_po", "district"]
    by_race: dict[tuple, list[str]] = {}
    for k, last, f in zip(map(tuple, c[rc].astype(str).values), c["last"],
                          c["first"]):
        by_race.setdefault(k + (last,), []).append(f)
    b_count: dict[tuple, int] = {}
    for k, variants in zip(map(tuple, b[rc].astype(str).values), b["lasts"]):
        for last, _ in variants:
            b_count[k + (last,)] = b_count.get(k + (last,), 0) + 1

    out = []
    for k, variants in zip(map(tuple, b[rc].astype(str).values), b["lasts"]):
        got = False
        for last, first in variants:
            firsts = by_race.get(k + (last,))
            if not firsts:
                continue
            if len(set(firsts)) == 1 and b_count.get(k + (last,), 0) == 1:
                got = True
                break
            if any(_compatible(g, first) for g in firsts):
                got = True
                break
        out.append(got)
    return out


def reverse_match(c: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """For each collection row, was that candidate on the general ballot?

    Returns on_ballot and general_votes. Same compatibility rule as match(),
    read the other way.
    """
    rc = ["year", "office", "state_po", "district"]
    by_race: dict[tuple, list[tuple[str, int]]] = {}
    for k, variants, v in zip(map(tuple, b[rc].astype(str).values), b["lasts"],
                              b["candidatevotes"]):
        for last, first in variants:
            by_race.setdefault(k + (last,), []).append((first, int(v)))
    ck = [tuple(x) + (l,) for x, l
          in zip(c[rc].astype(str).values, c["last"])]
    c_count: dict[tuple, int] = {}
    for k in ck:
        c_count[k] = c_count.get(k, 0) + 1

    hit, votes = [], []
    for k, f in zip(ck, c["first"]):
        cands = by_race.get(k)
        if not cands:
            hit.append(False), votes.append(pd.NA)
            continue
        if len({x[1] for x in cands}) == 1 and c_count.get(k, 0) == 1:
            hit.append(True), votes.append(cands[0][1])
            continue
        ok = [v for g, v in cands if _compatible(g, f)]
        hit.append(bool(ok))
        votes.append(max(ok) if ok else pd.NA)
    return pd.DataFrame({"on_ballot": hit, "general_votes": votes},
                        index=c.index)


def annotate(b: pd.DataFrame) -> None:
    """Write on_ballot / general_votes into the roster and the compat panel."""
    targets = [
        (REPO / "data/deliverable/panel_icpsr_compat.csv",
         "candidate_icpsr", "state", "district_id"),
        (REPO / "data/deliverable/release_roster.csv",
         "candidate", "state", "district"),
        (REPO / "quality_reports/coverage_audit/csv/candidate_crosswalk.csv",
         "candidate_icpsr", "state", "district_id"),
    ]
    panel_map: dict | None = None
    for path, namecol, statecol, distcol in targets:
        if not path.exists():
            print(f"  {path.name}: MISSING, skipped")
            continue
        d = pd.read_csv(path, low_memory=False)
        w = d.rename(columns={statecol: "state_po"}).copy()
        col = w[distcol]
        if pd.api.types.is_numeric_dtype(col):
            dd = col                      # already 3, 12, NaN for Senate
        else:
            # "LA03" -> 3. Guard against a float that reached here as "3.0",
            # where a trailing-digit regex would silently return 0.
            dd = col.astype(str).str.replace(r"\.0$", "", regex=True) \
                    .str.extract(r"(\d+)$")[0]
        w["district"] = pd.to_numeric(dd, errors="coerce").fillna(0).astype(int)
        w.loc[w.office == "senate", "district"] = 0
        w = keys(w, namecol, comma=True)
        res = reverse_match(w, b)
        d["on_ballot"] = res.on_ballot.values
        d["general_votes"] = res.general_votes.values

        K = ["year", "office", "state_po", "district", "last", "fi"]
        wk = list(map(tuple, w[K].astype(str).values))
        if panel_map is None:
            # The panel carries clean "Last, First" names, so its match is the
            # better one. Record it, and let the roster inherit it below.
            panel_map = {k: (h, v) for k, h, v
                         in zip(wk, res.on_ballot, res.general_votes)}
        else:
            fixed = 0
            for i, k in enumerate(wk):
                got = panel_map.get(k)
                if got is not None and got[0] != res.on_ballot.iat[i]:
                    d.iloc[i, d.columns.get_loc("on_ballot")] = got[0]
                    d.iloc[i, d.columns.get_loc("general_votes")] = got[1]
                    fixed += 1
            print(f"  {path.name:26} {fixed} rows taken from the panel's match "
                  f"(raw FEC display names are noisier)")

        d.to_csv(path, index=False)
        print(f"  {path.name:26} on_ballot true for "
              f"{int(d.on_ballot.sum()):,}/{len(d):,} "
              f"({100*d.on_ballot.mean():.1f}%)")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    b = ballot()
    print(f"ballot rows (D/R, general, 2002-2024): {len(b):,}")

    t, o = theirs(), ours()
    print(f"ICPSR general-election candidate-years : {len(t):,}")
    print(f"this release candidate-years           : {len(o):,}\n")

    b["in_icpsr"] = match(b, t)
    b["in_ours"] = match(b, o)

    rows = []
    for (office, year), g in b.groupby(["office", "year"]):
        for src, col in (("icpsr", "in_icpsr"), ("extension", "in_ours")):
            if not g[col].any():
                continue
            rows.append({
                "office": office, "year": year, "source": src,
                "n_ballot": len(g), "n_matched": int(g[col].sum()),
                "pct_cand": 100 * g[col].mean(),
                "votes_total": int(g.candidatevotes.sum()),
                "votes_matched": int(g.loc[g[col], "candidatevotes"].sum()),
                "pct_votes": 100 * g.loc[g[col], "candidatevotes"].sum()
                / g.candidatevotes.sum(),
            })
    r = pd.DataFrame(rows).sort_values(["office", "source", "year"])
    r.to_csv(OUT / "coverage_vs_ballot.csv", index=False)

    for src, g in r.groupby("source"):
        print(f"--- {src} ---")
        for _, x in g.iterrows():
            print(f"  {x.office:<6} {x.year}  {x.n_matched:>4}/{x.n_ballot:<4} "
                  f"= {x.pct_cand:5.1f}%   votes {x.pct_votes:5.1f}%")

    # How many collection rows are not on the ballot? Read from the same
    # matcher, not a stricter key, or the number is an artifact of the key.
    for lbl, c in (("ICPSR", t), ("ours", o)):
        rev = reverse_match(c, b)
        miss = int((~rev.on_ballot).sum())
        print(f"\n{lbl}: {miss:,}/{len(c):,} ({100*miss/len(c):.1f}%) not on a "
              f"general-election ballot")
    print(f"\nwrote {OUT/'coverage_vs_ballot.csv'}")
    print("\nannotating the released tables with on_ballot / general_votes:")
    annotate(b)
    return 0


if __name__ == "__main__":
    sys.exit(main())
