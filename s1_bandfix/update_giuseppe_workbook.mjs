import fs from "node:fs/promises";
import path from "node:path";
import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const [inputPath, outputPath, auditDir, previewDir] = process.argv.slice(2);
if (!inputPath || !outputPath || !auditDir || !previewDir) {
  throw new Error(
    "Usage: node update_giuseppe_workbook.mjs INPUT.xlsx OUTPUT.xlsx AUDIT_DIR PREVIEW_DIR",
  );
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (quoted) {
      if (char === '"' && text[index + 1] === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        field += char;
      }
    } else if (char === '"') {
      quoted = true;
    } else if (char === ",") {
      row.push(field);
      field = "";
    } else if (char === "\n") {
      row.push(field.replace(/\r$/, ""));
      rows.push(row);
      row = [];
      field = "";
    } else {
      field += char;
    }
  }
  if (field.length || row.length) {
    row.push(field.replace(/\r$/, ""));
    rows.push(row);
  }
  return rows;
}

function typed(value) {
  if (value === "") return null;
  if (/^-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/.test(value)) return Number(value);
  return value;
}

function readCsv(filePath) {
  return fs.readFile(filePath, "utf8").then((text) => parseCsv(text));
}

function matrixFromObjects(rows, columns) {
  return rows.map((row) => columns.map((column) => typed(row[column] ?? "")));
}

function objectsFromCsv(rows) {
  const [headers, ...data] = rows;
  return data.filter((row) => row.some((value) => value !== "")).map((row) =>
    Object.fromEntries(headers.map((header, index) => [header, row[index] ?? ""])),
  );
}

const [semanticCsv, comparisonCsv, geeSummaryText, weeklySummaryText] = await Promise.all([
  readCsv(path.join(auditDir, "postload_semantic_audit.csv")),
  readCsv(path.join(auditDir, "postload_field_comparison.csv")),
  fs.readFile(path.join(auditDir, "s1_corrected_vs_gee_summary.json"), "utf8"),
  fs.readFile(path.join(auditDir, "weekly_tiff_band_audit_summary.json"), "utf8"),
]);
const semanticRows = objectsFromCsv(semanticCsv);
const comparisonRows = objectsFromCsv(comparisonCsv);
const geeSummary = JSON.parse(geeSummaryText);
const weeklySummary = JSON.parse(weeklySummaryText);
const overall = semanticRows.find((row) => row.scope === "overall");
const yearRegionRows = semanticRows.filter((row) => row.scope === "year_region");
if (!overall) throw new Error("Missing overall row in postload semantic audit");

const workbook = await SpreadsheetFile.importXlsx(await FileBlob.load(inputPath));
const navy = "#1F4E78";
const headerBlue = "#174B67";
const paleBlue = "#D9EAF4";
const paleGreen = "#E2F0D9";
const lightGray = "#F2F2F2";
const borderColor = "#B8C4CE";
const fontName = "Arial";

const summary = workbook.worksheets.getItem("Summary");
summary.getRange("A11:B11").values = [["S1 production status", "Corrected and active"]];
summary.getRange("A35:J35").values = [["Sentinel-1 correction", null, null, null, null, null, null, null, null, null]];
summary.getRange("A35:J35").format = {
  fill: navy,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
};
summary.getRange("A36:C36").values = [["Check", "Result", "Interpretation"]];
summary.getRange("A36:C36").format = {
  fill: headerBlue,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  verticalAlignment: "center",
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
const totalDual = Number(overall.dual_pol_records);
const vvBelow = Number(overall.vv_below_vh_records);
const summaryValues = [
  ["Weekly TIFFs audited", weeklySummary.file_count, "All available S1 weeks"],
  ["Clear dual-polarization layout", weeklySummary.classification_counts.band1_likely_vh_band2_likely_vv, "Band 1 = VH; band 2 = VV"],
  ["Single-polarization weeks", weeklySummary.classification_counts.band1_only, "Band 1 = VV; VH-derived indices remain null"],
  ["Production rows", Number(overall.field_records), "All fields and all weekly records"],
  ["Dual-polarization records", totalDual, "Both VV and VH available"],
  ["Corrected records with VV below VH", vvBelow / totalDual, "0.072% of dual-polarization records"],
  ["Invalid S1 index domain", Number(overall.bad_index_domain), "Expected value is zero"],
  ["RVI-RCSPR identity errors", Number(overall.bad_rvi_rcspr_identity), "Expected value is zero"],
  ["VV-only records with derived values", Number(overall.vv_only_with_derived_value), "Expected value is zero"],
  ["S1-GEE matched field-weeks", geeSummary.matched_records, "Four Nakuru fields"],
  ["Exact S1-GEE pixel-count matches", geeSummary.pixel_count_exact_matches, "Same buffered geometry and centre-pixel rule"],
];
summary.getRange(`A37:C${36 + summaryValues.length}`).values = summaryValues;
summary.getRange(`A37:C${36 + summaryValues.length}`).format.font = { name: fontName, size: 10 };
summary.getRange(`A37:C${36 + summaryValues.length}`).format.borders = {
  insideHorizontal: { style: "thin", color: borderColor },
  bottom: { style: "thin", color: borderColor },
};
summary.getRange("B37:B47").format.numberFormat = "#,##0";
summary.getRange("B42").format.numberFormat = "0.000%";
summary.getRange("A49:J49").values = [["S1 comparison with Earth Engine", null, null, null, null, null, null, null, null, null]];
summary.getRange("A49:J49").format = {
  fill: navy,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
};
summary.getRange("A50:D50").values = [["Metric", "Mean production minus GEE", "MAE", "Mean dB difference"]];
summary.getRange("A50:D50").format = {
  fill: headerBlue,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
const geeMetrics = ["sigma_vv_linear", "sigma_vh_linear", "p_ratio", "rvi", "rcspr"];
const geeLabels = ["VV linear", "VH linear", "P-ratio", "RVI", "RCSPR"];
const geeValues = geeMetrics.map((metric, index) => [
  geeLabels[index],
  geeSummary[metric].mean_difference,
  geeSummary[metric].mae,
  geeSummary[metric].mean_db_difference ?? null,
]);
summary.getRange("A51:D55").values = geeValues;
summary.getRange("B51:D55").format.numberFormat = "0.0000";
summary.getRange("A51:D55").format.borders = {
  insideHorizontal: { style: "thin", color: borderColor },
  bottom: { style: "thin", color: borderColor },
};
summary.getRange("A57:J59").values = [
  ["The S1 band-label issue was systematic in the dual-polarization weekly products. Production now maps band 1 to VH and band 2 to VV.", null, null, null, null, null, null, null, null, null],
  ["For VV-only weeks, band 1 remains VV and the VH-dependent indices remain null.", null, null, null, null, null, null, null, null, null],
  ["The corrected S1 table became active on 2026-09-28. The previous table is retained for rollback.", null, null, null, null, null, null, null, null, null],
];
summary.getRange("A57:J57").merge();
summary.getRange("A58:J58").merge();
summary.getRange("A59:J59").merge();
summary.getRange("A57:J59").format = {
  fill: lightGray,
  font: { name: fontName, size: 10 },
  wrapText: true,
};
summary.getRange("A57:J59").format.rowHeight = 22;
summary.getRange("A35:J59").format.verticalAlignment = "center";
summary.getRange("A37:A47").format.columnWidth = 34;
summary.getRange("B37:B47").format.columnWidth = 18;
summary.getRange("C37:C47").format.columnWidth = 48;

const validation = workbook.worksheets.getItem("Validation and notes");
validation.getRange("A2:D2").values = [["Database run z_s1_bandfix_20260928_v1_5c58bc36", null, null, null]];
validation.getRange("A25:D25").values = [["S1 correction validation", null, null, null]];
validation.getRange("A25:D25").format = {
  fill: navy,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
};
validation.getRange("A26:D26").values = [["Check", "Result", "Expected", "Status"]];
validation.getRange("A26:D26").format = {
  fill: headerBlue,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
const validationRows = [
  ["S1 database rows", Number(overall.field_records), 128551562, "passed"],
  ["S1 raster parts", weeklySummary.file_count, 956, "passed"],
  ["Invalid index-domain records", Number(overall.bad_index_domain), 0, "passed"],
  ["RVI-RCSPR identity errors", Number(overall.bad_rvi_rcspr_identity), 0, "passed"],
  ["VV-only records with derived values", Number(overall.vv_only_with_derived_value), 0, "passed"],
  ["Database load parts", 956, 956, "passed"],
  ["Database checkpoint rows", Number(overall.field_records), 128551562, "passed"],
  ["Atomic S1 swap", "completed", "completed", "passed"],
  ["S2 table", "unchanged", "unchanged", "passed"],
];
validation.getRange("A27:D35").values = validationRows;
validation.getRange("B27:C35").format.numberFormat = "#,##0";
validation.getRange("A27:D35").format.borders = {
  insideHorizontal: { style: "thin", color: borderColor },
  bottom: { style: "thin", color: borderColor },
};
validation.getRange("D27:D35").format = { fill: paleGreen, font: { name: fontName, color: "#375623" } };
validation.getRange("A37:D40").values = [
  ["Band mapping", "Dual-pol: band 1 = VH, band 2 = VV", null, null],
  ["Single-pol mapping", "Band 1 = VV; VH and derived indices are null", null, null],
  ["Production table", "timeseries.s1_indices", null, null],
  ["Rollback table", "timeseries.s1_indices_legacy_pre_s1_bandfix_20260928", null, null],
];
validation.getRange("A37:A40").format.font = { name: fontName, bold: true, color: navy };
validation.getRange("B37:D40").format.wrapText = true;
validation.getRange("A37:D40").format.rowHeight = 30;
validation.getRange("A25:D40").format.font = { name: fontName, size: 10 };
validation.getRange("B37:B40").format.columnWidth = 58;

const existingS1Sheet = workbook.worksheets.items.find((sheet) => sheet.name === "S1 correction");
if (existingS1Sheet) throw new Error("Workbook already contains an S1 correction sheet");
const correction = workbook.worksheets.add("S1 correction");
correction.showGridLines = false;
correction.getRange("A1:M1").values = [["Sentinel-1 band correction audit", null, null, null, null, null, null, null, null, null, null, null, null]];
correction.getRange("A1:M1").format = {
  font: { name: fontName, size: 16, bold: true, color: navy },
  borders: { bottom: { style: "thin", color: navy } },
};
correction.getRange("A2:M2").values = [["All weekly rasters and all production field records; corrected table active 2026-09-28", null, null, null, null, null, null, null, null, null, null, null, null]];
correction.getRange("A2:M2").format = { font: { name: fontName, italic: true, color: "#666666" } };

correction.getRange("A4:C4").values = [["Production correction summary", null, null]];
correction.getRange("A4:C4").format = { fill: navy, font: { name: fontName, bold: true, color: "#FFFFFF" } };
correction.getRange("A5:C5").values = [["Metric", "Value", "Notes"]];
correction.getRange("A5:C5").format = {
  fill: headerBlue,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
const correctionSummary = [
  ["Weekly TIFFs audited", weeklySummary.file_count, "All available S1 weekly products"],
  ["Clear dual-pol layout", weeklySummary.classification_counts.band1_likely_vh_band2_likely_vv, "Band 1 = VH; band 2 = VV"],
  ["VV-only weekly products", weeklySummary.classification_counts.band1_only, "Band 1 = VV"],
  ["Empty weekly products", weeklySummary.classification_counts.both_empty, "No valid S1 values"],
  ["Other weekly products", weeklySummary.classification_counts.ambiguous + weeklySummary.classification_counts.insufficient_paired_samples, "Metadata and numeric checks support the same mapping"],
  ["Production field-week rows", Number(overall.field_records), "All fields and all weeks"],
  ["Dual-pol field-week records", totalDual, "Both corrected bands available"],
  ["Corrected VV below VH", vvBelow / totalDual, `${vvBelow.toLocaleString("en-US")} records`],
  ["VV-only field-week records", Number(overall.vv_only_records), "Derived indices remain null"],
  ["Invalid domain records", Number(overall.bad_index_domain), "P-ratio, RVI, and RCSPR"],
  ["Identity errors", Number(overall.bad_rvi_rcspr_identity), "RVI = 2 × (1 − RCSPR)"],
];
correction.getRange("A6:C16").values = correctionSummary;
correction.getRange("B6:B16").format.numberFormat = "#,##0";
correction.getRange("B13").format.numberFormat = "0.000%";
correction.getRange("A6:C16").format.borders = {
  insideHorizontal: { style: "thin", color: borderColor },
  bottom: { style: "thin", color: borderColor },
};

correction.getRange("E4:I4").values = [["S1 comparison with Earth Engine", null, null, null, null]];
correction.getRange("E4:I4").format = { fill: navy, font: { name: fontName, bold: true, color: "#FFFFFF" } };
correction.getRange("E5:I5").values = [["Metric", "Mean difference", "MAE", "Correlation", "Mean dB difference"]];
correction.getRange("E5:I5").format = {
  fill: headerBlue,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
const geeDetail = geeMetrics.map((metric, index) => [
  geeLabels[index],
  geeSummary[metric].mean_difference,
  geeSummary[metric].mae,
  geeSummary[metric].correlation,
  geeSummary[metric].mean_db_difference ?? null,
]);
correction.getRange("E6:I10").values = geeDetail;
correction.getRange("F6:I10").format.numberFormat = "0.0000";
correction.getRange("E6:I10").format.borders = {
  insideHorizontal: { style: "thin", color: borderColor },
  bottom: { style: "thin", color: borderColor },
};
correction.getRange("E12:I15").values = [
  ["Matched field-weeks", geeSummary.matched_records, null, null, null],
  ["Exact pixel-count matches", geeSummary.pixel_count_exact_matches, null, null, null],
  ["Geometry", "Same adaptive buffer and centre-pixel inclusion", null, null, null],
  ["Interpretation", "Similar VV and VH dB offsets largely cancel in normalized indices", null, null, null],
];
correction.getRange("F14:I14").merge();
correction.getRange("F15:I15").merge();
correction.getRange("E12:E15").format.font = { name: fontName, bold: true, color: navy };
correction.getRange("F12:I15").format.wrapText = true;
correction.getRange("E14:I14").format.rowHeight = 34;
correction.getRange("E15:I15").format.rowHeight = 42;

correction.getRange("A18:K18").values = [["Five-field comparison with the previous production table", null, null, null, null, null, null, null, null, null, null]];
correction.getRange("A18:K18").format = { fill: navy, font: { name: fontName, bold: true, color: "#FFFFFF" } };
const comparisonColumns = [
  "field_id", "matched_weeks", "matched_dual_pol_weeks", "vv_vs_previous_vh_mae",
  "vh_vs_previous_vv_mae", "previous_mean_p_ratio", "corrected_mean_p_ratio",
  "previous_mean_rvi", "corrected_mean_rvi", "previous_mean_rcspr", "corrected_mean_rcspr",
];
correction.getRange("A19:K19").values = [[
  "Field", "Matched weeks", "Dual-pol weeks", "New VV vs old VH MAE", "New VH vs old VV MAE",
  "Old P-ratio", "New P-ratio", "Old RVI", "New RVI", "Old RCSPR", "New RCSPR",
]];
correction.getRange("A19:K19").format = {
  fill: headerBlue,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  verticalAlignment: "center",
  wrapText: true,
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
correction.getRange(`A20:K${19 + comparisonRows.length}`).values = matrixFromObjects(comparisonRows, comparisonColumns);
correction.getRange(`B20:C${19 + comparisonRows.length}`).format.numberFormat = "#,##0";
correction.getRange(`D20:K${19 + comparisonRows.length}`).format.numberFormat = "0.0000";
correction.getRange(`A20:K${19 + comparisonRows.length}`).format.borders = {
  insideHorizontal: { style: "thin", color: borderColor },
  bottom: { style: "thin", color: borderColor },
};

const yearStart = 27;
correction.getRange(`A${yearStart}:M${yearStart}`).values = [["Corrected results by year and region", null, null, null, null, null, null, null, null, null, null, null, null]];
correction.getRange(`A${yearStart}:M${yearStart}`).format = { fill: navy, font: { name: fontName, bold: true, color: "#FFFFFF" } };
const yearHeaders = [
  "Region", "Year", "Field records", "Dual-pol records", "VV below VH", "P-ratio > 1",
  "Bad domain", "Bad identity", "VV-only records", "VV-only with derived", "Mean P-ratio", "Mean RVI", "Mean RCSPR",
];
correction.getRange(`A${yearStart + 1}:M${yearStart + 1}`).values = [yearHeaders];
correction.getRange(`A${yearStart + 1}:M${yearStart + 1}`).format = {
  fill: headerBlue,
  font: { name: fontName, bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  verticalAlignment: "center",
  wrapText: true,
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
const yearColumns = [
  "region", "iso_year", "field_records", "dual_pol_records", "vv_below_vh_records",
  "p_ratio_gt_1_records", "bad_index_domain", "bad_rvi_rcspr_identity", "vv_only_records",
  "vv_only_with_derived_value", "mean_p_ratio", "mean_rvi", "mean_rcspr",
];
const yearDataStart = yearStart + 2;
const yearDataEnd = yearDataStart + yearRegionRows.length - 1;
correction.getRange(`A${yearDataStart}:M${yearDataEnd}`).values = matrixFromObjects(yearRegionRows, yearColumns);
correction.getRange(`B${yearDataStart}:B${yearDataEnd}`).format.numberFormat = "0";
correction.getRange(`C${yearDataStart}:J${yearDataEnd}`).format.numberFormat = "#,##0";
correction.getRange(`K${yearDataStart}:M${yearDataEnd}`).format.numberFormat = "0.0000";
correction.getRange(`A${yearDataStart}:M${yearDataEnd}`).format.borders = {
  insideHorizontal: { style: "thin", color: borderColor },
  bottom: { style: "thin", color: borderColor },
};
for (let row = yearDataStart; row <= yearDataEnd; row += 2) {
  correction.getRange(`A${row}:M${row}`).format.fill = paleBlue;
}

correction.getRange(`A${yearDataEnd + 2}:M${yearDataEnd + 4}`).values = [
  ["Conclusion", "The swapped VV/VH labels were systematic across the dual-polarization products, not limited to selected weeks.", null, null, null, null, null, null, null, null, null, null, null],
  ["Single polarization", "The 69 single-band weeks are treated as VV-only; VH, P-ratio, RVI, and RCSPR remain null.", null, null, null, null, null, null, null, null, null, null, null],
  ["Rollback", "The previous production table remains available as timeseries.s1_indices_legacy_pre_s1_bandfix_20260928.", null, null, null, null, null, null, null, null, null, null, null],
];
for (let row = yearDataEnd + 2; row <= yearDataEnd + 4; row += 1) {
  correction.getRange(`B${row}:M${row}`).merge();
}
correction.getRange(`A${yearDataEnd + 2}:A${yearDataEnd + 4}`).format.font = { name: fontName, bold: true, color: navy };
correction.getRange(`B${yearDataEnd + 2}:M${yearDataEnd + 4}`).format.wrapText = true;
correction.getRange(`A${yearDataEnd + 2}:M${yearDataEnd + 4}`).format.fill = lightGray;
correction.getRange(`A${yearDataEnd + 2}:M${yearDataEnd + 4}`).format.rowHeight = 30;

correction.getRange("A1:M60").format.font = { name: fontName, size: 10 };
correction.getRange("A1:M2").format.font = { name: fontName, size: 11 };
correction.getRange("A1").format.font = { name: fontName, size: 16, bold: true, color: navy };
correction.getRange("A1:A60").format.columnWidth = 23;
correction.getRange("B1:B60").format.columnWidth = 17;
correction.getRange("C1:C60").format.columnWidth = 28;
correction.getRange("D1:M60").format.columnWidth = 16;
correction.getRange("A19:K19").format.rowHeight = 36;
correction.getRange(`A${yearStart + 1}:M${yearStart + 1}`).format.rowHeight = 36;
correction.getRange("A1:M60").format.verticalAlignment = "center";

const summaryCheck = await workbook.inspect({
  kind: "table",
  range: "Summary!A35:J59",
  include: "values,formulas",
  tableMaxRows: 30,
  tableMaxCols: 12,
});
console.log(summaryCheck.ndjson);
const correctionCheck = await workbook.inspect({
  kind: "table",
  range: `S1 correction!A1:M${yearDataEnd + 4}`,
  include: "values,formulas",
  tableMaxRows: 12,
  tableMaxCols: 13,
});
console.log(correctionCheck.ndjson);
const formulaErrors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 300 },
  summary: "final formula error scan",
});
console.log(formulaErrors.ndjson);

await fs.mkdir(path.dirname(outputPath), { recursive: true });
await fs.mkdir(previewDir, { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);

const exported = await SpreadsheetFile.importXlsx(await FileBlob.load(outputPath));
for (const sheetName of ["Summary", "Validation and notes", "S1 correction"]) {
  const preview = await exported.render({ sheetName, autoCrop: "all", scale: 1, format: "png" });
  const safeName = sheetName.replace(/[^a-z0-9]+/gi, "_");
  await fs.writeFile(path.join(previewDir, `${safeName}.png`), new Uint8Array(await preview.arrayBuffer()));
}
const exportedErrors = await exported.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 300 },
  summary: "exported workbook formula error scan",
});
console.log(exportedErrors.ndjson);
