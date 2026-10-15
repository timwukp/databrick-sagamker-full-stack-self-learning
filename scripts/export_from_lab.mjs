// Export the synthetic dataset embedded in lab/retail-3d.html into plain files.
//
//   node scripts/export_from_lab.mjs
//
// The 3D lab page is the single source of truth. This script loads its <script>
// in a sandbox with minimal browser stubs, reads the DATA / NODES / PLANES objects,
// and writes:
//   data/<platform>/<NN>_<step>/<role>_<name>.{csv,json,txt}   sample rows
//   data/<platform>/<NN>_<step>/<role>_<name>.schema.json      column list
//   data/manifest.json                                         index of every file
//   docs/data-walkthrough.md                                   every step, before and after
//   docs/databricks-vs-aws-mapping.md                          component map for both stacks
// Run it again after editing the lab page; verify.py checks that the outputs exist.

import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const li = process.argv.indexOf("--lang");
const LANG = li > 0 ? process.argv[li + 1] : "zh";   // "en": read en/lab, write docs/en, skip data/
const EN = LANG === "en";
const LAB_FILE = EN ? path.join(ROOT, "en", "lab", "retail-3d.html") : path.join(ROOT, "lab", "retail-3d.html");
const DOCS = EN ? path.join(ROOT, "docs", "en") : path.join(ROOT, "docs");
const LAB_LINK = EN ? "../../en/lab/retail-3d.html" : "../lab/retail-3d.html";
const L = EN ? { changed: "What this step changed", key: "Key points" } : { changed: "這一步改了什麼", key: "學習重點" };
const html = fs.readFileSync(LAB_FILE, "utf8");
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
let main = scripts.find(s => s.includes("const DATA = {"));
if (!main) throw new Error("data script not found in lab/retail-3d.html");
main = main.trimEnd();
if (!main.endsWith("})();")) throw new Error("unexpected script shape");
main = main.slice(0, -5) + "\nglobalThis.__x = {DATA, NODES, PLANES, STAGES, STEP_ORDER, GROUP_NAME, GROUP_NOTE, dataFor, info, D};\n})();";

// ---- minimal browser stubs so the page script can boot headless ----
const el = () => ({ innerHTML: "", textContent: "", value: "", hidden: false, style: {}, dataset: {},
  classList: { toggle() {}, add() {}, remove() {} }, setAttribute() {}, addEventListener() {},
  querySelector() { return null; }, querySelectorAll() { return []; }, scrollIntoView() {},
  getBoundingClientRect() { return { left: 0, top: 0, width: 1200, height: 620 }; },
  setPointerCapture() {}, parentElement: null, getContext() { return ctx; } });
const ctx = new Proxy({}, { get(t, k) { if (k === "measureText") return s => ({ width: String(s).length * 6 }); return k in t ? t[k] : () => {}; },
  set(t, k, v) { t[k] = v; return true; } });
const store = {};
const canvas = el(); canvas.parentElement = el();
const sandbox = {
  document: { getElementById: id => id === "scene" ? canvas : (store[id] ||= el()), querySelector: q => (store[q] ||= el()), querySelectorAll: () => [] },
  window: { matchMedia: () => ({ matches: true }), devicePixelRatio: 1, addEventListener() {} },
  matchMedia: () => ({ matches: true }), ResizeObserver: class { observe() {} },
  requestAnimationFrame: () => 0, performance: { now: () => 0 },
  location: { hash: "" }, history: { replaceState() {} }, URLSearchParams, console
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(main, sandbox);
const { DATA, NODES, PLANES, STAGES, STEP_ORDER, GROUP_NAME, GROUP_NOTE, dataFor, info, D } = sandbox.__x;

// ---- helpers ----
const PLATS = { dbx: "databricks", aws: "aws" };
const PNAME = { dbx: "Databricks", aws: "AWS" };
const byId = Object.fromEntries(NODES.map(n => [n.id, n]));
const slug = s => s.replace(/\s*\(.*$/, "").split("/").filter(Boolean).pop()
  .toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 60) || "data";
const stepTitle = (id, plat) => id.startsWith("p:") ? `Layer: ${GROUP_NAME[id.slice(2)]}`
  : `${STAGES[byId[id].s]}: ${info(byId[id], plat).t}`;
const csvCell = v => {
  if (v === null || v === undefined) return "";
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
const mdCell = v => {
  if (v === null || v === undefined) return "*null*";
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  return s.replace(/\|/g, "\\|").replace(/\n/g, " ");
};
const cols = t => (t.cols || []).map(c => ({ name: c[0], type: c[1], tag: c[2] || null, note: c[3] || null }));
const ensure = d => fs.mkdirSync(d, { recursive: true });

// clean previous export
if (!EN) for (const p of Object.values(PLATS)) fs.rmSync(path.join(ROOT, "data", p), { recursive: true, force: true });

const manifest = [];
function writeObj(obj, plat, stepNo, id, role) {
  if (!obj || EN) return null;
  const dir = path.join("data", PLATS[plat], `${String(stepNo).padStart(2, "0")}_${id.replace("p:", "layer_")}`);
  ensure(path.join(ROOT, dir));
  const base = `${role}_${slug(obj.name)}`;
  let file;
  if (obj.kind === "table") {
    file = `${base}.csv`;
    const lines = [obj.cols.map(c => csvCell(c[0])).join(",")].concat(obj.rows.map(r => r.map(csvCell).join(",")));
    fs.writeFileSync(path.join(ROOT, dir, file), lines.join("\n") + "\n");
  } else if (obj.kind === "json") {
    file = `${base}.json`;
    const body = obj.items.length === 1 ? obj.items[0].obj : obj.items.map(it => ({ label: it.label, flag: it.flag || null, document: it.obj }));
    fs.writeFileSync(path.join(ROOT, dir, file), JSON.stringify(body, null, 2) + "\n");
  } else {
    const ext = /\.csv\.out\b/.test(obj.name) ? "csv.out" : /\.csv\b/.test(obj.name) ? "csv" : "txt";
    file = `${base}.${ext}`;
    fs.writeFileSync(path.join(ROOT, dir, file), obj.text + "\n");
  }
  const schemaFile = `${base}.schema.json`;
  fs.writeFileSync(path.join(ROOT, dir, schemaFile), JSON.stringify({ name: obj.name, kind: obj.kind, columns: cols(obj) }, null, 2) + "\n");
  const rec = { step: id, step_no: stepNo, platform: PLATS[plat], role, name: obj.name, kind: obj.kind,
    file: `${dir}/${file}`, schema_file: `${dir}/${schemaFile}`, columns: (obj.cols || []).length,
    rows: obj.kind === "table" ? obj.rows.length : obj.kind === "json" ? obj.items.length : obj.text.split("\n").length };
  manifest.push(rec);
  return rec;
}

// ---- data files + walkthrough markdown ----
let md = [];
md.push("# Data walkthrough: every step, before and after", "",
  "Generated from `lab/retail-3d.html` by `scripts/export_from_lab.mjs`. The same content is interactive in the",
  `[3D lab](${LAB_LINK}#data). Sample rows are synthetic; columns, types and system fields follow the official docs.`, "",
  "Legend for row notes: duplicate, fails check, late, schema change, changed, look here, alert, superseded.", "");
md.push("## Steps", "");
STEP_ORDER.forEach((id, k) => md.push(`${k + 1}. [${stepTitle(id, "dbx")}](#step-${k + 1})`));
md.push("");

function mdTable(t) {
  const out = [];
  if (!t) return ["_No input: this is where the data starts (a customer system outside the platform)._", ""];
  out.push(`**${t.name}**`, "");
  if (t.cols && t.cols.length) {
    out.push("| column | type | tag | note |", "|---|---|---|---|");
    t.cols.forEach(c => out.push(`| \`${c[0]}\` | ${mdCell(c[1])} | ${c[2] || ""} | ${mdCell(c[3] || "")} |`));
    out.push("");
  }
  if (t.kind === "table") {
    const flags = t.flags || {};
    const hasF = Object.keys(flags).length > 0;
    out.push("| " + (hasF ? "row note | " : "") + t.cols.map(c => c[0]).join(" | ") + " |");
    out.push("|" + (hasF ? "---|" : "") + t.cols.map(() => "---").join("|") + "|");
    t.rows.forEach((r, i) => out.push("| " + (hasF ? (flags[i] ? mdCell(`${flags[i][0]}: ${flags[i][1]}`) : "") + " | " : "") + r.map(mdCell).join(" | ") + " |"));
    if (t.foot) out.push("", `_${t.foot}_`);
  } else if (t.kind === "json") {
    t.items.forEach(it => { if (it.label) out.push(`- ${it.flag ? `**${it.flag}** ` : ""}${it.label}`); });
    out.push("", "```json", JSON.stringify(t.items.length === 1 ? t.items[0].obj : t.items.map(i => i.obj), null, 2), "```");
  } else {
    out.push("```text", t.text, "```");
  }
  out.push("");
  return out;
}

STEP_ORDER.forEach((id, k) => {
  const stepNo = k + 1;
  md.push(`<a id="step-${stepNo}"></a>`, `## ${stepNo}. ${stepTitle(id, "dbx")}`, "");
  for (const plat of ["dbx", "aws"]) {
    const d = dataFor(id, plat);
    writeObj(d.in, plat, stepNo, id, "before");
    writeObj(d.out, plat, stepNo, id, "after");
    d.extra.forEach((t, i) => writeObj(t, plat, stepNo, id, `after${i + 2}`));
    md.push(`### On ${PNAME[plat]}: ${stepTitle(id, plat)}`, "", d.what || "", "");
    md.push("#### Before", "", ...mdTable(d.in));
    md.push("#### After", "", ...mdTable(d.out));
    d.extra.forEach(t => md.push(...mdTable(t)));
    md.push(`#### ${L.changed}`, "", d.why || "", "");
    if (d.code) md.push(`#### Code (${d.code.lang})`, "", "```", d.code.text, "```", "");
    md.push(`[Open this step in the 3D lab](${LAB_LINK}#step=${id}&plat=${plat})`, "");
  }
});
fs.writeFileSync(path.join(DOCS, "data-walkthrough.md"), md.join("\n"));

// ---- component map ----
const map = ["# Databricks vs AWS: component map", "",
  "Generated from the 3D lab. Each row is one box in the diagram; the boxes sit in the same place on both stacks.", "",
  `| step | Databricks | AWS | ${L.key} |`, "|---|---|---|---|"];
const OWN = { dbx: "Databricks", aws: "AWS", oss: "open source / third party", cust: "customer system", out: "business outcome" };
NODES.slice().sort((a, b) => a.s - b.s || a.l - b.l).forEach(n => {
  const a = info(n, "dbx"), b = info(n, "aws");
  const cell = i => `**${mdCell(i.t)}** (${OWN[i.o]})<br>${mdCell(i.d || "")}${i.doc ? `<br>[doc](${i.doc})` : ""}`;
  map.push(`| ${n.s + 1}. ${STAGES[n.s]} | ${cell(a)} | ${cell(b)} | ${mdCell(n.n || "")} |`);
});
map.push("", "## Layers under and over every step", "", `| layer | Databricks | AWS | ${L.key} |`, "|---|---|---|---|");
for (const g of ["ws", "gov", "store", "found"]) {
  const t = pl => PLANES[pl].filter(x => x.group === g).map(x => `**${mdCell(x.t)}**<br>${mdCell(x.d)}${x.doc ? `<br>[doc](${x.doc})` : ""}`).join("<br><br>");
  map.push(`| ${GROUP_NAME[g]} | ${t("dbx")} | ${t("aws")} | ${mdCell(GROUP_NOTE[g])} |`);
}
fs.writeFileSync(path.join(DOCS, "databricks-vs-aws-mapping.md"), map.join("\n") + "\n");

// ---- raw source files used by labs/01_retail_medallion.py ----
if (!EN) {
ensure(path.join(ROOT, "data", "raw"));
fs.writeFileSync(path.join(ROOT, "data", "raw", "pos_events.jsonl"),
  DATA.pos.out.items.map(it => JSON.stringify(it.obj)).join("\n") + "\n");
fs.writeFileSync(path.join(ROOT, "data", "raw", "promo_SEA.csv"), DATA.ext.out.text + "\n");
fs.writeFileSync(path.join(ROOT, "data", "raw", "sap_mard.csv"),
  [DATA.erp.out.cols.map(c => c[0]).join(",")].concat(DATA.erp.out.rows.map(r => r.join(","))).join("\n") + "\n");

fs.writeFileSync(path.join(ROOT, "data", "manifest.json"), JSON.stringify({
  generated_from: "lab/retail-3d.html", generator: "scripts/export_from_lab.mjs",
  steps: STEP_ORDER.map((id, k) => ({ step_no: k + 1, step: id, title_databricks: stepTitle(id, "dbx"), title_aws: stepTitle(id, "aws") })),
  files: manifest }, null, 2) + "\n");

}
console.log(EN ? `exported English docs to docs/en` : `exported ${manifest.length} files across ${STEP_ORDER.length} steps`);
