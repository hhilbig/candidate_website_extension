"""Crosswalk from the mayoral finalists to the American Local Government Elections Database (ALGED).

ALGED (de Benedictis-Kessner, Lee, Velez and Warshaw; OSF project mv5e6, file ledb_candidatelevel.csv, CC BY 4.0)
covers mayoral elections 1989-2021. The crosswalk gives, for each finalist, the ALGED candidate ID (ledb_candid) and
contest ID of the same person in the same election. It copies no ALGED values; users join ALGED themselves.

Match rule: same place FIPS code (our city_id; three consolidated governments use ALGED's code, LEDB_CITY_CODE), same year, ALGED office Mayor, and the same person by name.
A name matches ("name") when the last names agree after normalization and a first name agrees, where a first name
also counts as its quoted nickname or its long form (Jim = James), or when the full names have a token-sort similarity
of at least 90. Otherwise a match on the last name alone ("last_name") counts if only one ALGED person in that city and
year has that last name (ALGED gives some candidates by last name only). If the person appears in several ALGED
contests of that year (a first round and a runoff), the contest in the month of our election date is kept, else the
nearest earlier month. Finalists without a match keep empty IDs and a reason.

Usage:
  python scripts/build_mayoral_ledb_crosswalk.py --finalists build/<release>/mayoral_finalists.csv \
      --ledb <ledb_candidatelevel.csv> --output <crosswalk.csv>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

LEDB_FIRST_YEAR, LEDB_LAST_YEAR = 1989, 2021
# Our place codes for the census balance or CDP part of a consolidated government -> ALGED's code for the same city.
LEDB_CITY_CODE = {"1571550": "1517000", "1836003": "1836000", "2148006": "2148000"}  # Honolulu, Indianapolis, Louisville
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "md", "phd"}


NICKNAMES = {"jim": "james", "jimmy": "james", "steve": "stephen", "tom": "thomas", "bob": "robert", "rob": "robert",
             "bill": "william", "mike": "michael", "andy": "andrew", "chuck": "charles", "marty": "martin",
             "joe": "joseph", "dan": "daniel", "dave": "david", "tony": "anthony", "ed": "edward", "rick": "richard",
             "dick": "richard", "ron": "ronald", "greg": "gregory", "jeff": "jeffrey", "ken": "kenneth",
             "kenny": "kenneth", "zack": "zachary", "pete": "peter", "sam": "samuel", "matt": "matthew",
             "chris": "christopher", "nick": "nicholas", "tim": "timothy", "liz": "elizabeth", "betsy": "elizabeth",
             "kate": "katherine", "kathy": "katherine", "sue": "susan", "pat": "patrick", "larry": "lawrence"}


def clean(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z\"' ]", " ", text.replace("-", " "))


def tokens(name: str) -> list[str]:
    """Name tokens without nicknames in quotes or brackets and without suffixes."""
    plain = re.sub(r"\"[^\"]*\"|'[^']*'|\([^)]*\)", " ", clean(name))
    return [t for t in re.sub(r"[^a-z ]", " ", plain).split() if t not in SUFFIXES and len(t) > 1]


def first_names(name: str) -> set[str]:
    """The first name, any nickname in quotes, and the long form of a short name."""
    found = set(tokens(name)[:1]) | set(re.findall(r"[\"']([a-z]+)[\"']", clean(name)))
    return found | {NICKNAMES[n] for n in found if n in NICKNAMES}


def match_basis(ours: str, theirs: str, last_name_unique: bool) -> str:
    """'name' if the same person by full name, 'last_name' if only the last name agrees and is unique, else ''."""
    a, b = tokens(ours), tokens(theirs)
    if not a or not b or a[-1] != b[-1]:
        return "name" if a and b and fuzz.token_sort_ratio(" ".join(a), " ".join(b)) >= 90 else ""
    if len(b) == 1:
        return "last_name" if last_name_unique else ""
    mine, other = first_names(ours), first_names(theirs)
    if any(x.startswith(y) or y.startswith(x) for x in mine for y in other):
        return "name"
    return "last_name" if last_name_unique else ""


def crosswalk(finalists: pd.DataFrame, ledb_path: Path) -> pd.DataFrame:
    """One row per finalist: candidate_election_id, ledb_candid, ledb_contest, ledb_match."""
    ledb = pd.read_csv(ledb_path, low_memory=False)
    ledb = ledb[ledb["office_consolidated"] == "Mayor"].copy()
    ledb["city_id"] = ledb["fips"].astype(int).astype(str).str.zfill(7)
    groups = {key: g for key, g in ledb.groupby(["city_id", "year"])}
    rows = []
    for f in finalists.itertuples():
        year, month = int(f.election_date[:4]), int(f.election_date[5:7])
        row = {"candidate_election_id": f.candidate_election_id, "ledb_candid": "", "ledb_contest": "",
               "ledb_match": ""}
        if not LEDB_FIRST_YEAR <= year <= LEDB_LAST_YEAR:
            row["ledb_match"] = "outside_ledb_years"
        elif (LEDB_CITY_CODE.get(f.city_id, f.city_id), year) not in groups:
            row["ledb_match"] = "no_ledb_election"
        else:
            g = groups[(LEDB_CITY_CODE.get(f.city_id, f.city_id), year)]
            last = g["full_name"].map(lambda n: (tokens(n) or [""])[-1])
            people = g.groupby(last)["ledb_candid"].nunique()  # ALGED persons for each last name in this city-year
            basis = pd.Series([match_basis(f.candidate_name, n, people[l] == 1) for n, l in zip(g["full_name"], last)],
                              index=g.index)
            hits = g[basis == "name"] if (basis == "name").any() else g[basis == "last_name"]
            kind = "name" if (basis == "name").any() else "last_name"
            if hits.empty:
                row["ledb_match"] = "no_ledb_candidate"
            elif hits["ledb_candid"].nunique() > 1:
                row["ledb_match"] = "ambiguous"
            else:
                months = hits["month"].fillna(0).astype(int)
                exact = hits[months == month]
                pick = exact if not exact.empty else hits[months <= month].sort_values("month").tail(1)
                pick = pick if not pick.empty else hits.sort_values("month").head(1)
                row.update(ledb_candid=str(int(pick["ledb_candid"].iloc[0])), ledb_contest=pick["contest"].iloc[0],
                           ledb_match=f"{kind}_{'same_month' if not exact.empty else 'other_month'}")
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--finalists", type=Path, required=True)
    parser.add_argument("--ledb", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    out = crosswalk(pd.read_csv(args.finalists, dtype=str), args.ledb)
    out.to_csv(args.output, index=False)
    print(json.dumps({"finalists": len(out), "ledb_sha256": hashlib.sha256(args.ledb.read_bytes()).hexdigest(),
                      "match": out["ledb_match"].value_counts().to_dict()}, indent=1))


if __name__ == "__main__":
    main()
