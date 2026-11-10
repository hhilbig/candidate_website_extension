#!/usr/bin/env python3
"""Build the RA human-validation workbook (Excel) for the released v2.2 extraction.

Precision sheets: one row per released record, shown in a short passage with the
record's evidence marked. The coder never sees the model's kind, field, or
entity type; those go to a separate answer key for scoring.

Recall sheets: short passages split into one sentence (issues, biography) or one
line (endorsements) per row. The coder labels each row without seeing any model
output. Scoring matches coded rows to released evidence offsets.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import re
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.worksheet.datavalidation import DataValidation

REPO = Path(__file__).resolve().parent.parent
RELEASE = REPO / "build/hive_candidate_extraction/release_20260930_v22_model"
BUNDLE = REPO / "build/hive_candidate_extraction/bundle_20260924_v22"
SEED = 20261030
CONTEXT_CHARS = 250
RECALL_PASSAGE_CHARS = 900
RECALL_MIN_WORDS = 4  # issue/biography sentences shorter than this are menu text

ISSUE_POSITION = [
    "Yes",
    "No: not a position (description/slogan/prediction)",
    "No: about someone else (opponent/party/group)",
    "No: menu/link/ad text",
]
ISSUE_CLAIM = [
    "Yes",
    "No: claim adds something the text does not say",
    "No: claim leaves out the main point",
    "No: claim reverses the text",
]
ISSUE_KIND = ["support", "oppose", "proposal", "past action", "none of these"]
BIO_ABOUT = [
    "Yes",
    "No: not a background fact (policy/slogan/praise)",
    "No: about a family member",
    "No: about someone else",
    "No: not stated in the text",
]
BIO_FIELDS = [
    "occupation",
    "education",
    "elected office",
    "other public service",
    "military service",
    "local roots",
    "none of these",
]
BIO_VALUE = ["Yes", "No: the text says something else", "No: fact is incomplete or garbled"]
END_SUPPORT = [
    "Yes",
    "No: name appears but not as an endorser",
    "Can't tell from the page",
]
END_TYPE = ["individual", "organization"]
END_NAME = ["Yes", "No: cut off or merged with other text", "No: not a name"]
YES_NO = ["Yes", "No"]
BIO_RECALL = BIO_FIELDS[:-1] + ["several facts (list them in Notes)", "no background fact"]

FIELD_LABEL = {
    "occupation": "occupation",
    "education": "education",
    "elected_office_experience": "elected office",
    "public_service_experience": "other public service",
    "military_service": "military service",
    "local_roots": "local roots",
}
KIND_LABEL = {"past_action": "past action"}

GUIDE = [
    ("How to code", ""),
    ("", "Each row shows a claim, fact, or endorser from a campaign website. The source text is "
         "between >>> and <<<. Answer from the passage only. Do not use outside knowledge."),
    ("", "Leave Q2 and Q3 blank if Q1 is not 'Yes'. If unsure, give your best answer and write "
         "'unsure' in Notes. Sheets 4-6: answer every white row. Grey rows start a new passage."),
    ("1 Issues", ""),
    ("Q1 Yes", "The candidate states a direction on an issue: supports, opposes, proposes, did, "
               "or is doing something. 'I am pro-life' and 'Protecting Social Security' count."),
    ("Q1 No: not a position", "A topic label ('Immigration'), a slogan ('Fighting for you'), a "
                              "description, a prediction, or a claim too vague to say what "
                              "the candidate wants."),
    ("Q1 No: about someone else", "The opponent's or another group's view or record."),
    ("Q1 No: menu/link/ad text", "Navigation, link titles, donation or volunteer appeals."),
    ("Q2", "Does the claim say what the marked text says? No if it adds, leaves out the main "
           "point, or reverses the text."),
    ("Q3", "support = favors something, including general promises. proposal = a specific plan "
           "('a bill to cap insulin at $35'). past action = did it. none of these = is doing "
           "it now."),
    ("2 Biography", ""),
    ("Q1 Yes", "A concrete fact about the candidate: a job, degree, office, public service, "
               "military service, or tie to a place."),
    ("Q1 No: not a background fact", "A policy view, slogan, or vague praise. Party, religion, "
                                     "the campaign, and a membership alone also do not count."),
    ("Q1 No: other options", "About a family member, about someone else, or not in the text."),
    ("Q2", "Use the whole passage. elected office only if the text says the office is elected; "
           "otherwise occupation. local roots needs a stated tie ('born in', 'moved to'). If "
           "two types fit, pick one and write the other in Notes."),
    ("Q3", "Use only the marked text. Yes if it states every part of the fact."),
    ("3 Endorsements", ""),
    ("Q1", "Yes if the page presents the name as backing the candidate (endorsement heading, "
           "/endorsements page, quote of support). Any person counts. Ratings, awards, donors, "
           "and comments are No. If the 'Where the name appears' column gives only a page "
           "address, open the link. Pick 'Can't tell' if it still does not show."),
    ("Q3", "Judge the name column only. A title, office, or place is not part of the name. "
           "A name printed with an initial ('Patricia M') is complete."),
    ("4-6 Sentences and lines", ""),
    ("", "Same rules as sheets 1-3. These sheets find items the dataset missed. On sheet 5, "
         "pick 'several facts' and list them in Notes if a sentence has more than one."),
]
def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_parent_texts(parent_ids: set[str]) -> dict[str, str]:
    """Rebuild page text for the given parents from the bundle's segment texts."""
    parts: dict[str, list[tuple[int, int, str]]] = {p: [] for p in parent_ids}
    expected: dict[str, str] = {}
    with gzip.open(BUNDLE / "segments_enriched.jsonl.gz", "rt", encoding="utf-8") as handle:
        for line in handle:
            if '"parent_document_id"' not in line:
                continue
            row = json.loads(line)
            pid = row["parent_document_id"]
            if pid in parts:
                parts[pid].append((int(row["segment_start_char"]), int(row["segment_end_char"]),
                                   row["source_text"]))
                expected[pid] = row["extraction_source_text_sha256"]
    texts = {}
    for pid, segs in parts.items():
        size = max(end for _, end, _ in segs)
        chars = [""] * size
        for start, _, text in segs:
            chars[start:start + len(text)] = list(text)
        text = "".join(chars)
        assert sha(text) == expected[pid], f"page text hash mismatch: {pid}"
        texts[pid] = text
    return texts


def snap_left(text: str, pos: int) -> int:
    """Move left to the start of a line or sentence."""
    lo = max(0, pos)
    cut = max(text.rfind("\n", 0, lo), *(text.rfind(p, 0, lo) for p in (". ", "! ", "? ")))
    return 0 if cut < 0 else cut + (1 if text[cut] == "\n" else 2)


def snap_right(text: str, pos: int) -> int:
    hits = [i for i in (text.find("\n", pos), *(text.find(p, pos) for p in (". ", "! ", "? ")))
            if i >= 0]
    return min(hits) + 1 if hits else len(text)


def endorsement_reason(rec: dict) -> str:
    """Say in plain words where the page marks the name as an endorsement."""
    context = rec["endorsement_context_evidence"]
    path = rec["endorsement_context_qualifying_original_path"]
    if rec["support_mode"] == "explicit_text":
        return "The marked text itself says the person or group supports the candidate."
    if isinstance(context, str) and context:
        return f"The name is listed under this text on the page: \"{context}\""
    if isinstance(path, str) and path:
        return f"The name is on a page whose address ends in: {path}"
    return "The page or section is an endorsement list (check the page link)."


def tidy(text: str) -> str:
    text = re.sub(r"[ \t\u00a0]+", " ", text)
    return re.sub(r"\n\s*\n+", "\n", text).strip()


def passage(text: str, start: int, end: int) -> str:
    left = snap_left(text, start - CONTEXT_CHARS)
    right = snap_right(text, end + CONTEXT_CHARS)
    return tidy(text[left:start] + " >>> " + text[start:end] + " <<< " + text[end:right])


def split_units(text: str, start: int, end: int, by_line: bool) -> list[tuple[int, int]]:
    """Return (start, end) offsets of lines, or sentences within lines."""
    units = []
    for m in re.finditer(r"[^\n]+", text[start:end]):
        ls, le = start + m.start(), start + m.end()
        if by_line:
            units.append((ls, le))
            continue
        cursor = ls
        for b in re.finditer(r"[.!?][\"”’)]?\s+", text[ls:le]):
            units.append((cursor, ls + b.end()))
            cursor = ls + b.end()
        if cursor < le:
            units.append((cursor, le))
    out = []
    for s, e in units:
        while s < e and text[s].isspace():
            s += 1
        while e > s and text[e - 1].isspace():
            e -= 1
        if e > s:
            out.append((s, e))
    return out


def sample_records(df: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    # One row per distinct record (the same record repeats across page captures).
    keys = sorted(df["candidate_record_key"].unique())
    chosen = set(rng.sample(keys, n))
    return df[df["candidate_record_key"].isin(chosen)].drop_duplicates("candidate_record_key")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-precision", type=int, default=10)
    ap.add_argument("--n-recall-passages", type=int, default=2)
    ap.add_argument("--out-dir", type=Path,
                    default=REPO / "validation/ra_validation_pilot_2026-10-30")
    args = ap.parse_args()
    rng = random.Random(SEED)

    docs = pd.read_parquet(RELEASE / "candidate_extraction_documents.parquet")
    docs = docs.set_index("parent_document_id")
    tables = {
        "issues": pd.read_parquet(RELEASE / "candidate_issue_statements.parquet"),
        "biography": pd.read_parquet(RELEASE / "candidate_biography_facts.parquet"),
        "endorsements": pd.read_parquet(RELEASE / "candidate_endorsements.parquet"),
    }
    span = {"issues": "evidence", "biography": "evidence", "endorsements": "record_evidence"}

    picks = {r: sample_records(t, args.n_precision, rng) for r, t in tables.items()}
    recall_parents = {}
    for route, table in tables.items():
        pool = sorted(table["parent_document_id"].unique())
        recall_parents[route] = rng.sample(pool, args.n_recall_passages * 4)  # spares for retries
    needed = {p for t in picks.values() for p in t["parent_document_id"]}
    needed |= {p for ps in recall_parents.values() for p in ps}
    texts = load_parent_texts(needed)

    def meta(pid: str) -> dict:
        d = docs.loc[pid]
        urls = list(d["wayback_urls"]) if d["wayback_urls"] is not None else []
        return {
            "candidate": d["candidate_icpsr"],
            "race": f"{d['state']} {d['office']} {d['year']} ({d['party']})",
            "page": urls[0] if urls else "",
        }

    key_rows = []
    sheets: dict[str, list[dict]] = {}
    for route, pick in picks.items():
        rows = []
        col = span[route]
        for i, rec in enumerate(pick.itertuples(index=False), 1):
            rec = rec._asdict()
            text = texts[rec["parent_document_id"]]
            s, e = int(rec[f"{col}_parent_start_char"]), int(rec[f"{col}_parent_end_char"])
            assert text[s:e] == rec[col], f"evidence offset mismatch: {rec['record_id']}"
            row_id = f"{route[:3].upper()}-{i:03d}"
            row = {"row_id": row_id, **meta(rec["parent_document_id"]),
                   "passage": passage(text, s, e)}
            if route == "issues":
                row["claim"] = rec["claim"]
                model = KIND_LABEL.get(rec["statement_kind"], rec["statement_kind"] or "none of these")
            elif route == "biography":
                row["fact"] = rec["value"]
                model = FIELD_LABEL[rec["field"]]
            else:
                row["endorser"] = rec["endorser_name"]
                row["why"] = endorsement_reason(rec)
                model = rec["entity_type"]
            rows.append(row)
            key_rows.append({"sheet": route, "row_id": row_id, "record_id": rec["record_id"],
                             "parent_document_id": rec["parent_document_id"],
                             "model_label": model})
        sheets[route] = rows

    for route, parents in recall_parents.items():
        rows, made = [], 0
        by_line = route == "endorsements"
        recs = tables[route]
        col = span[route]
        for pid in parents:
            if made == args.n_recall_passages:
                break
            text = texts[pid]
            start = snap_left(text, rng.randrange(0, max(1, len(text) - RECALL_PASSAGE_CHARS)))
            end = snap_right(text, start + RECALL_PASSAGE_CHARS)
            units = [u for u in split_units(text, start, end, by_line)
                     if by_line or len(text[u[0]:u[1]].split()) >= RECALL_MIN_WORDS]
            if len(units) < 4:
                continue
            made += 1
            prec = recs[recs["parent_document_id"] == pid]
            spans = list(zip(prec[f"{col}_parent_start_char"].astype(int),
                             prec[f"{col}_parent_end_char"].astype(int), prec["record_id"]))
            m = meta(pid)
            pass_id = f"{route[:3].upper()}-P{made}"
            path = re.sub(r"^https?://web\.archive\.org/web/[^/]+/", "", m["page"])
            rows.append({"header": f"Passage {pass_id}: {m['candidate']}, {m['race']}, page {path}",
                         "page": m["page"]})
            for j, (us, ue) in enumerate(units, 1):
                row_id = f"{pass_id}-{j:02d}"
                rows.append({"row_id": row_id, "text": tidy(text[us:ue])})
                hits = [rid for s, e, rid in spans if s < ue and e > us]
                key_rows.append({"sheet": f"{route}_missed", "row_id": row_id, "record_id": ";".join(hits),
                                 "parent_document_id": pid, "unit_start": us, "unit_end": ue,
                                 "model_label": len(hits)})
        sheets[f"{route}_missed"] = rows

    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_workbook(sheets, args.out_dir / "ra_validation_pilot.xlsx")
    pd.DataFrame(key_rows).to_csv(args.out_dir / "answer_key_DO_NOT_SHARE.csv", index=False)
    (args.out_dir / "build_manifest.json").write_text(json.dumps({
        "release_dir": str(RELEASE.relative_to(REPO)),
        "release_manifest_sha256": sha((RELEASE / "candidate_extraction_manifest.json").read_text()),
        "seed": SEED, "n_precision": args.n_precision,
        "n_recall_passages": args.n_recall_passages,
        "recall_min_words": RECALL_MIN_WORDS, "context_chars": CONTEXT_CHARS,
    }, indent=2) + "\n")


HEAD_FILL = PatternFill("solid", fgColor="DCE6F1")
ANSWER_FILL = PatternFill("solid", fgColor="FFF2CC")
PASSAGE_FILL = PatternFill("solid", fgColor="EDEDED")
WRAP = Alignment(wrap_text=True, vertical="top")


def add_sheet(wb: Workbook, title: str, columns: list[tuple[str, str, int, list | None]],
              rows: list[dict]) -> None:
    """columns: (key, header, width, dropdown options or None for display/free text)."""
    ws = wb.create_sheet(title)
    for c, (_, header, width, opts) in enumerate(columns, 1):
        cell = ws.cell(row=1, column=c, value=header)
        cell.font, cell.alignment = Font(bold=True), WRAP
        ws.row_dimensions[1].height = 60
        cell.fill = ANSWER_FILL if opts is not None or header == "Notes" else HEAD_FILL
        ws.column_dimensions[cell.column_letter].width = width
    ws.freeze_panes = "B2"
    r = 2
    for row in rows:
        if "header" in row:  # recall passage separator
            cell = ws.cell(row=r, column=1, value=row["header"])
            cell.font, cell.fill = Font(bold=True), PASSAGE_FILL
            link = ws.cell(row=r, column=2, value="open page")
            link.hyperlink, link.font = row["page"], Font(color="0563C1", underline="single")
            r += 1
            continue
        for c, (key, _, _, _) in enumerate(columns, 1):
            value = row.get(key, "")
            cell = ws.cell(row=r, column=c, value=value if key != "page" else "open page")
            cell.alignment = WRAP
            if key == "page" and value:
                cell.hyperlink, cell.font = value, Font(color="0563C1", underline="single")
        # openpyxl does not fit row heights; estimate wrapped lines per cell.
        lines = max(sum(len(part) // max(1, int(width * 1.1)) + 1
                        for part in str(row.get(key, "")).split("\n"))
                    for key, _, width, _ in columns)
        ws.row_dimensions[r].height = min(409, 15 * lines)
        r += 1
    answer_rows = [i for i, row in enumerate(rows, 2) if "header" not in row]
    for c, (_, _, _, opts) in enumerate(columns, 1):
        if opts:
            letter = ws.cell(row=1, column=c).column_letter
            dv = DataValidation(type="list", formula1='"' + ",".join(opts) + '"',
                                allow_blank=True, showErrorMessage=True)
            ws.add_data_validation(dv)
            for i in answer_rows:  # no dropdown on passage separator rows
                dv.add(f"{letter}{i}")


def write_workbook(sheets: dict[str, list[dict]], path: Path) -> None:
    for opts in (ISSUE_POSITION, ISSUE_CLAIM, ISSUE_KIND, BIO_ABOUT, BIO_FIELDS, BIO_VALUE,
                 END_SUPPORT, END_TYPE, END_NAME, BIO_RECALL):
        # Excel list validation: no commas inside options, 255 characters in total.
        assert all("," not in o for o in opts) and len(",".join(opts)) <= 255
    wb = Workbook()
    guide = wb.active
    guide.title = "Read me first"
    guide.column_dimensions["A"].width = 30
    guide.column_dimensions["B"].width = 110
    for i, (a, b) in enumerate(GUIDE, 1):
        guide.cell(row=i, column=1, value=a).font = Font(bold=not b)
        guide.cell(row=i, column=2, value=b).alignment = WRAP
        if not b:
            guide.cell(row=i, column=1).fill = HEAD_FILL
    base = [("row_id", "Row", 9, None), ("candidate", "Candidate", 18, None),
            ("race", "Race", 18, None), ("page", "Page", 10, None),
            ("passage", "Passage (source text between >>> <<<)", 70, None)]
    add_sheet(wb, "1 Issues", base + [
        ("claim", "Claim", 35, None),
        ("q1", "Q1 Is the claim a policy position of the candidate?", 24, ISSUE_POSITION),
        ("q2", "Q2 If Q1 = Yes: does the marked text support the claim?", 24, ISSUE_CLAIM),
        ("q3", "Q3 If Q1 = Yes: what kind of claim is it?", 16, ISSUE_KIND),
        ("notes", "Notes", 25, None)], sheets["issues"])
    add_sheet(wb, "2 Biography", base + [
        ("fact", "Fact", 30, None),
        ("q1", "Q1 Is this a background fact about the candidate?", 22, BIO_ABOUT),
        ("q2", "Q2 If Q1 = Yes: which type of fact is it?", 18, BIO_FIELDS),
        ("q3", "Q3 If Q1 = Yes: does the marked text support the fact?", 20, BIO_VALUE),
        ("notes", "Notes", 25, None)], sheets["biography"])
    add_sheet(wb, "3 Endorsements", base + [
        ("endorser", "Endorser name", 25, None),
        ("why", "Where the name appears", 25, None),
        ("q1", "Q1 Does the page present the endorser as endorsing the candidate?", 24, END_SUPPORT),
        ("q2", "Q2 Person or organization?", 14, END_TYPE),
        ("q3", "Q3 Is the endorser name complete and correct?", 22, END_NAME),
        ("notes", "Notes", 25, None)], sheets["endorsements"])
    recall_base = [("row_id", "Row", 13, None), ("page", "Page", 10, None),
                   ("text", "Sentence or line", 80, None)]
    add_sheet(wb, "4 Issues sentences", recall_base + [
        ("q1", "Does this sentence state a policy position of the candidate?", 18, YES_NO),
        ("notes", "Notes", 25, None)], sheets["issues_missed"])
    add_sheet(wb, "5 Biography sentences", recall_base + [
        ("q1", "Which background fact does this sentence state about the candidate?", 26, BIO_RECALL),
        ("notes", "Notes", 25, None)], sheets["biography_missed"])
    add_sheet(wb, "6 Endorsement lines", recall_base + [
        ("q1", "Does this line name a person or group endorsing the candidate?", 18, YES_NO),
        ("notes", "Notes", 25, None)], sheets["endorsements_missed"])
    wb.save(path)


if __name__ == "__main__":
    main()
