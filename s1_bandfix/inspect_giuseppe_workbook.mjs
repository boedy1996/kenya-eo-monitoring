import fs from "node:fs/promises";
import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const inputPath = process.argv[2];
const outputDir = process.argv[3];
if (!inputPath || !outputDir) {
  throw new Error("Usage: node inspect_giuseppe_workbook.mjs INPUT.xlsx OUTPUT_DIR");
}

await fs.mkdir(outputDir, { recursive: true });
const workbook = await SpreadsheetFile.importXlsx(await FileBlob.load(inputPath));
const overview = await workbook.inspect({
  kind: "workbook,sheet,table,drawing",
  maxChars: 12000,
  tableMaxRows: 8,
  tableMaxCols: 12,
  tableMaxCellChars: 100,
});
console.log(overview.ndjson);

const sheets = await workbook.inspect({ kind: "sheet", include: "id,name", maxChars: 8000 });
console.log(sheets.ndjson);
for (const sheet of workbook.worksheets.items) {
  const safeName = sheet.name.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "");
  const preview = await workbook.render({
    sheetName: sheet.name,
    autoCrop: "all",
    scale: 1,
    format: "png",
  });
  await fs.writeFile(`${outputDir}/${safeName}.png`, new Uint8Array(await preview.arrayBuffer()));
}
