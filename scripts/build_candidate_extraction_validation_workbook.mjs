#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";


function argValue(name) {
  const index = process.argv.indexOf(name);
  if (index < 0 || index + 1 >= process.argv.length) {
    throw new Error(`missing required argument ${name}`);
  }
  return process.argv[index + 1];
}


function columnName(index) {
  let value = index + 1;
  let result = "";
  while (value) {
    const remainder = (value - 1) % 26;
    result = String.fromCharCode(65 + remainder) + result;
    value = Math.floor((value - 1) / 26);
  }
  return result;
}


function addCodingSheet(workbook, name, rows, tableName, codingColumns) {
  const sheet = workbook.worksheets.add(name);
  sheet.showGridLines = false;
  if (!rows.length) throw new Error(`${name} has no rows`);
  const headers = Object.keys(rows[0]);
  const values = [headers, ...rows.map((row) => headers.map((header) => row[header] ?? ""))];
  const endColumn = columnName(headers.length - 1);
  sheet.getRange(`A1:${endColumn}${values.length}`).values = values;
  sheet.getRange(`A1:${endColumn}1`).format = {
    fill: "#1F4E78",
    font: { bold: true, color: "#FFFFFF" },
    wrapText: true,
    verticalAlignment: "center",
  };
  sheet.getRange(`A2:${endColumn}${values.length}`).format = {
    verticalAlignment: "top",
  };
  sheet.getRange(`A1:${endColumn}${values.length}`).format.borders = {
    insideHorizontal: { style: "thin", color: "#D9E2F3" },
  };
  const table = sheet.tables.add(`A1:${endColumn}${values.length}`, true, tableName);
  table.style = "TableStyleMedium2";
  table.showBandedRows = true;
  table.showFilterButton = true;
  sheet.freezePanes.freezeRows(1);
  sheet.freezePanes.freezeColumns(4);

  headers.forEach((header, index) => {
    const letter = columnName(index);
    const range = sheet.getRange(`${letter}2:${letter}${values.length}`);
    if (codingColumns.includes(header)) {
      range.dataValidation = { rule: { type: "list", values: ["yes", "no"] } };
      range.format.fill = "#FFF2CC";
      range.format.columnWidth = 18;
    } else if (["source_text", "source_context", "model_record_json", "model_records_json", "coder_notes", "missed_records_json"].includes(header)) {
      range.format.wrapText = true;
      range.format.columnWidth = header === "source_text" ? 80 : 55;
    } else {
      range.format.columnWidth = 18;
    }
  });
  sheet.getRange(`A2:${endColumn}${values.length}`).format.rowHeight = 72;
  return sheet;
}


const precisionPath = argValue("--precision-json");
const recallPath = argValue("--recall-json");
const outputPath = argValue("--output-xlsx");
const precision = JSON.parse(await fs.readFile(precisionPath, "utf8"));
const recall = JSON.parse(await fs.readFile(recallPath, "utf8"));
const codingComplete =
  precision.every((row) => row.coding_status === "complete") &&
  recall.every((row) => row.coding_status === "complete");

const workbook = Workbook.create();
const instructions = workbook.worksheets.add("Instructions");
instructions.showGridLines = false;
instructions.getRange("A1:B1").merge();
instructions.getRange("A1").values = [[
  codingComplete
    ? "Candidate website extraction validation — completed audit"
    : "Candidate website extraction validation",
]];
instructions.getRange("A1:B1").format = {
  fill: "#1F4E78",
  font: { bold: true, color: "#FFFFFF", fontSize: 16 },
  verticalAlignment: "center",
};
instructions.getRange("A3:B13").values = codingComplete ? [
  ["Sheet", "Audit status"],
  ["Record precision", "Complete: 50 judgments were human-confirmed in chat; 25 were completed by Codex at Hanno's request."],
  ["Page recall", "Complete: all 15 pages were audited by Codex at Hanno's request."],
  ["", ""],
  ["Precision field", "Definition"],
  ["record_present_in_page", "The page states the proposed fact, position, or endorsement."],
  ["normalized_value_faithful", "The normalized model value preserves the meaning of the page."],
  ["record_type_correct", "The issue/biography/endorsement subtype is correct."],
  ["evidence_alone_supports_record", "The quoted evidence alone supports the full record without relying on nearby text or outside knowledge."],
  ["", ""],
  ["Provenance", "This is an AI-assisted audit, not a fully independent human validation. See coding_mode and the completion manifest."],
] : [
  ["Sheet", "What to code"],
  ["Record precision", "For each model record, enter yes or no in all four yellow columns."],
  ["Page recall", "Read the full page and enter whether all requested records were captured. List omissions as JSON when the answer is no."],
  ["", ""],
  ["Precision field", "Definition"],
  ["record_present_in_page", "The page states the proposed fact, position, or endorsement."],
  ["normalized_value_faithful", "The normalized model value preserves the meaning of the page."],
  ["record_type_correct", "The issue/biography/endorsement subtype is correct."],
  ["evidence_alone_supports_record", "The quoted evidence alone supports the full record without relying on nearby text or outside knowledge."],
  ["", ""],
  ["Allowed answers", "Use lowercase yes or no. Leave coding_status as pending until the row is finished, then change it to complete."],
];
instructions.getRange("A3:B3").format = { fill: "#D9EAF7", font: { bold: true } };
instructions.getRange("A7:B7").format = { fill: "#D9EAF7", font: { bold: true } };
instructions.getRange("A3:B13").format.wrapText = true;
instructions.getRange("A3:A13").format.columnWidth = 28;
instructions.getRange("B3:B13").format.columnWidth = 95;
instructions.getRange("A1:B1").format.rowHeight = 30;

addCodingSheet(
  workbook,
  "Record precision",
  precision,
  "RecordPrecisionTable",
  ["record_present_in_page", "normalized_value_faithful", "record_type_correct", "evidence_alone_supports_record"],
);
addCodingSheet(
  workbook,
  "Page recall",
  recall,
  "PageRecallTable",
  ["all_requested_records_captured"],
);

await fs.mkdir(path.dirname(outputPath), { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);

for (const [sheetName, range] of [
  ["Instructions", "A1:B13"],
  ["Record precision", `A1:${columnName(Object.keys(precision[0]).length - 1)}${Math.min(precision.length + 1, 8)}`],
  ["Page recall", `A1:${columnName(Object.keys(recall[0]).length - 1)}${Math.min(recall.length + 1, 6)}`],
]) {
  const preview = await workbook.render({ sheetName, range, scale: 1, format: "png" });
  const previewPath = `${outputPath}.${sheetName.replaceAll(" ", "_")}.png`;
  await fs.writeFile(previewPath, new Uint8Array(await preview.arrayBuffer()));
}

console.log((await workbook.inspect({
  kind: "sheet,table",
  maxChars: 3000,
  tableMaxRows: 4,
  tableMaxCols: 8,
})).ndjson);
console.log((await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 100 },
  summary: "final formula error scan",
})).ndjson);
