# Brief for a model coder: validation of extracted records (2026 data)

You are an independent coder. You check records that a language model extracted from
2026 campaign website pages of candidates for the U.S. House and Senate. Your answers
measure the quality of the extraction. Judge strictly and from the page text only. Do not
give the model the benefit of the doubt, and do not look for reasons to fail it.

## Tools

All commands run from `/Users/hanno/Documents/GitHub/candidate_website_extension`.
`TOOL` is `validation/release_validation_2026-11-11_inputs/coder_tools.py`. `TASK` is the task
file that your assignment names.

1. `python3 TOOL show TASK` prints the extraction rules, the review rules, and the questions
   with their help texts. Read all of it first. The help texts decide borderline cases.
2. `python3 TOOL item TASK N` prints item N (0-based). If the output is too long to read at
   once, redirect it to a file in your scratch folder and read that file in parts. Read the
   complete page text of each item.
3. `python3 TOOL answer TASK N '<JSON>'` stores your answers for item N. Store each item
   directly after you judge it. Do not collect answers in memory.

Code only the item numbers of your assignment. Do not run `finish`. Do not read any
`*_results.json` or `*.answers.jsonl` file, and do not read files of other validation folders.
Do not edit any file other than your scratch files.

## Answers

Precision task (one extracted record for each item): the JSON object holds one key for each
question, with the value `"yes"` or `"no"`. Add `"notes"` (one or two sentences) for each
`no`. Answer each question independently: one wrong field does not make the other
questions `no`.

Recall task (one complete page and all records that the model holds for it): the JSON
object holds `"all_requested_records_captured"` (`"yes"` or `"no"`). If `no`, it also holds
`"missed_records"`: a list with one object for each eligible record that the page states
and the model does not hold, each with `"type"`, `"value"`, and `"evidence"` (text copied
exactly from the page). Do not list a near-duplicate of a record that the model holds. A
page with no eligible record is complete. Apply the eligibility rules of `show` exactly:
a record is missed only if the rules require it.

## Report

When all your items have answers, reply with: the number of items coded, the count of `no`
answers for each question (or the count of incomplete pages and of missed records), and
the two or three most frequent defect types with one example each. Keep the reply under
250 words.
