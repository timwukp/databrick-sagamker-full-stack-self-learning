#!/usr/bin/env python3
"""Verification target for this site. Run before every commit:

    python3 verify.py

Standard library only. Exits non-zero on any failure and prints how many checks ran,
so a verifier that silently checks nothing is visible. Groups:

  structure   required files exist, Pages serves index.html from the repo root
  isolation   no external scripts, styles, images or network calls in the pages
  links       every relative link resolves; every lab deep link names a real step
  data        every manifest file exists and every CSV matches its schema
  numbers     the synthetic data reconciles across steps
  wording     neutral self-study wording only; no private data or credentials
  code        page JavaScript parses (if node is installed); the lab notebook compiles
"""
import csv
import json
import py_compile
import re
import shutil
import subprocess
import sys
import tempfile
from decimal import Decimal
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
checks = 0
failures = []


def check(cond, msg):
    global checks
    checks += 1
    if not cond:
        failures.append(msg)


def read(p):
    return (ROOT / p).read_text(encoding="utf-8")


# ---------- structure ----------
REQUIRED = [
    "index.html", "lab/retail-3d.html", "README.md", ".nojekyll",
    "docs/use-case.md", "docs/data-walkthrough.md", "docs/databricks-vs-aws-mapping.md",
    "docs/common-mistakes.md", "docs/talk-track.md", "docs/sources.md",
    "data/manifest.json", "data/raw/pos_events.jsonl", "data/raw/promo_SEA.csv", "data/raw/sap_mard.csv",
    "labs/01_retail_medallion.py", "scripts/export_from_lab.mjs", "scripts/build_portal.py",
    "scripts/publish.sh", ".github/workflows/site-verify.yml",
    "en/index.html", "en/lab/retail-3d.html", "i18n/en.json", "scripts/build_en.py",
    "docs/en/use-case.md", "docs/en/data-walkthrough.md", "docs/en/databricks-vs-aws-mapping.md",
    "docs/en/common-mistakes.md", "docs/en/talk-track.md", "docs/en/sources.md",
]
for p in REQUIRED:
    check((ROOT / p).is_file(), f"structure: missing {p}")

TOP_LEVEL = {".github", ".gitignore", ".nojekyll", "README.md", "index.html", "verify.py",
             "data", "docs", "lab", "labs", "scripts", ".git", "en", "i18n"}
for entry in ROOT.iterdir():
    if entry.name == ".DS_Store":
        continue
    check(entry.name in TOP_LEVEL, f"structure: unexpected top-level entry {entry.name} (would be published)")

PAGES = ["index.html", "lab/retail-3d.html", "en/index.html", "en/lab/retail-3d.html"]


class Collector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links, self.ids, self.h1, self.resources = [], set(), 0, []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if "id" in a:
            self.ids.add(a["id"])
        if tag == "h1":
            self.h1 += 1
        if tag == "a" and a.get("href"):
            self.links.append(a["href"])
        if tag in ("script", "img", "iframe", "source", "video", "audio") and a.get("src"):
            self.resources.append(a["src"])
        if tag == "link" and a.get("href"):
            self.resources.append(a["href"])


parsed = {}
for page in PAGES:
    if not (ROOT / page).is_file():
        continue
    c = Collector()
    c.feed(read(page))
    parsed[page] = c
    check(c.h1 == 1, f"structure: {page} should have exactly one h1, found {c.h1}")

# ---------- isolation ----------
for page, c in parsed.items():
    text = read(page)
    check(not c.resources, f"isolation: {page} loads external or extra resources: {c.resources[:3]}")
    for pat in (r"@import\s+url\(", r"\bfetch\(", r"XMLHttpRequest", r"new\s+WebSocket", r"url\(\s*['\"]?https?:"):
        check(not re.search(pat, text), f"isolation: {page} contains {pat}")

# ---------- links ----------
manifest = json.loads(read("data/manifest.json"))
step_ids = {s["step"] for s in manifest["steps"]}
for page, c in parsed.items():
    base = (ROOT / page).parent
    for href in c.links:
        if re.match(r"^(https?:|mailto:)", href):
            continue
        target, _, frag = href.partition("#")
        if target:
            check((base / target).resolve().exists(), f"links: {page} -> {href} does not exist")
        if not target and frag and "=" not in frag:
            check(frag in c.ids, f"links: {page} anchor #{frag} has no matching id")
        if "step=" in frag:
            sid = re.search(r"step=([^&]+)", frag).group(1)
            check(sid in step_ids, f"links: {page} deep link names unknown step {sid}")
lab_text = read("lab/retail-3d.html")
for sid in step_ids:
    check(f'"{sid}"' in lab_text, f"links: step {sid} not found in the lab page")

# ---------- data ----------
for f in manifest["files"]:
    fp, sp = ROOT / f["file"], ROOT / f["schema_file"]
    check(fp.is_file(), f"data: missing {f['file']}")
    check(sp.is_file(), f"data: missing {f['schema_file']}")
    if not (fp.is_file() and sp.is_file()):
        continue
    schema = json.loads(sp.read_text(encoding="utf-8"))
    if f["kind"] == "table":
        with fp.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
        check(rows and rows[0] == [c["name"] for c in schema["columns"]], f"data: header/schema mismatch in {f['file']}")
        check(len(rows) - 1 == f["rows"], f"data: row count mismatch in {f['file']}")
        check(all(len(r) == len(rows[0]) for r in rows[1:]), f"data: ragged CSV {f['file']}")
    elif f["kind"] == "json":
        try:
            json.loads(fp.read_text(encoding="utf-8"))
            ok = True
        except ValueError:
            ok = False
        check(ok, f"data: invalid JSON {f['file']}")
for line in read("data/raw/pos_events.jsonl").splitlines():
    try:
        json.loads(line)
        ok = True
    except ValueError:
        ok = False
    check(ok, "data: data/raw/pos_events.jsonl has an invalid line")


def table(platform, step_no, role):
    rec = next((f for f in manifest["files"] if f["platform"] == platform and f["step_no"] == step_no and f["role"] == role), None)
    check(rec is not None, f"numbers: no {platform} step {step_no} {role} table")
    if not rec:
        return []
    with (ROOT / rec["file"]).open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def step_no(sid):
    return next(s["step_no"] for s in manifest["steps"] if s["step"] == sid)


# ---------- numbers ----------
D = Decimal
for plat in ("databricks", "aws"):
    silver = table(plat, step_no("silver"), "after")
    check(len(silver) == 4, f"numbers: {plat} silver should have 4 lines")
    for r in silver:
        check(D(r["qty"]) * D(r["unit_price"]) - D(r["discount"]) == D(r["net_amount"]),
              f"numbers: {plat} silver net_amount wrong for {r['event_id']} line {r['line_no']}")
    check(not any(r["sku"] == "" for r in silver), f"numbers: {plat} silver kept a line without SKU")
    check(len({(r["event_id"], r["line_no"]) for r in silver}) == len(silver), f"numbers: {plat} silver has duplicates")
gold = table("databricks", step_no("gold"), "after")
price = {("SG", "SKU-20871"): D("6.90"), ("MY", "SKU-20871"): D("7.50"), ("SG", "SKU-10023"): D("3.95"), ("TH", "SKU-31045"): D("289")}
for r in gold:
    unit = price[(r["store_id"][:2], r["sku"])]
    factor = D("0.8") if r["is_promo"] == "true" else D("1")
    check((D(r["units"]) * unit * factor).quantize(D("0.01")) == D(r["net_revenue"]), f"numbers: gold revenue wrong for {r['store_id']} {r['sku']}")
inv_in = {r["item"]: r["value"] for r in table("databricks", step_no("inv"), "before")}
inv_out = table("databricks", step_no("inv"), "after")[0]
days = [int(x) for x in re.findall(r"\d+", inv_in["p50, next 7 days (forecast)"].split("=")[0])]
total = sum(days)
check(total == int(inv_in["p50, next 7 days (forecast)"].split("=")[1]), "numbers: 7-day p50 sum does not add up")
fc = table("databricks", step_no("batch"), "after")
check([int(float(r["p50"])) for r in fc if r["dc_id"] == "DC-SG-01"] == days[:4], "numbers: forecast table and 7-day sum disagree")
stock = {(r["dc_id"], r["sku"]): r for r in table("databricks", step_no("gold"), "after2")}
st = stock[("DC-SG-01", "SKU-20871")]
suggested = total + int(inv_in["safety_stock (policy)"]) - int(st["on_hand_eod"]) - int(st["open_po_qty"])
check(suggested == int(inv_out["suggested_qty"]), "numbers: suggested order quantity is wrong")
pack = int(inv_in["case_pack (supplier terms)"])
check(-(-suggested // pack) * pack == int(inv_out["order_qty"]), "numbers: order quantity is not rounded up to the case pack")
rt_in, rt_out = table("databricks", step_no("rtc"), "before"), table("databricks", step_no("rtc"), "after")[0]
check(sum(int(r["qty"]) for r in rt_in if r["event_ts_utc"] >= "2026-10-10 02:00:00") == int(rt_out["units_15m"]), "numbers: 15-minute window sum is wrong")
check(sum(int(r["qty"]) for r in rt_in) == int(rt_out["units_60m"]), "numbers: 60-minute window sum is wrong")
for plat in ("databricks", "aws"):
    trace = table(plat, step_no("genai"), "after")
    check(sum(int(r["latency_ms"]) for r in trace[1:]) == int(trace[0]["latency_ms"]), f"numbers: {plat} agent trace spans do not add up")
feat_dbx = table("databricks", step_no("fs_off"), "after")
feat_aws = table("aws", step_no("fs_off"), "after")
check([f"{r['store_id']}#{r['sku']}" for r in feat_dbx] == [r["store_sku"] for r in feat_aws], "numbers: feature group record ids do not match the feature table keys")

# ---------- wording ----------
PUBLISHED = PAGES + [str(p.relative_to(ROOT)) for p in (ROOT / "docs").rglob("*.md")] + ["README.md"]
# English pages and docs: no Chinese at all, except the label of the link back to the Chinese site
CJK = re.compile(r"[\u3000-\u303f\u3400-\u9fff\uff00-\uffef]")
for p in ["en/index.html", "en/lab/retail-3d.html"] + [str(x.relative_to(ROOT)) for x in (ROOT / "docs" / "en").glob("*.md")]:
    left = CJK.findall(read(p).replace("中文", ""))
    check(not left, f"wording: {p} still has Chinese text: {''.join(left[:20])}")
    if p.endswith(".html"):
        check('<html lang="en">' in read(p), f"wording: {p} should declare <html lang=\"en\">")
for zh, en in (("index.html", "en/index.html"), ("lab/retail-3d.html", "en/lab/retail-3d.html")):
    check('hreflang="en"' in read(zh), f"links: {zh} has no link to the English page")
    check('hreflang="zh-Hant"' in read(en), f"links: {en} has no link back to the Chinese page")
tr = json.loads(read("i18n/en.json"))
check(all(not CJK.search(v) and '"' not in v for v in tr.values()), "wording: i18n/en.json has Chinese or straight double quotes in a translation")
FORBIDDEN = [r"面試", r"\binterview", r"前東家", r"你的主場", r"\bHM\b", r"hiring manager"]
for p in PUBLISHED:
    text = read(p)
    for pat in FORBIDDEN:
        check(not re.search(pat, text, re.I), f"wording: {p} contains '{pat}'")
ALL_TEXT = [p for p in ROOT.rglob("*") if p.is_file() and ".git" not in p.parts and p.suffix in
            {".html", ".md", ".py", ".mjs", ".json", ".csv", ".jsonl", ".txt", ".sh", ".yml", ".out"}]
SECRETS = [r"github_pat_[A-Za-z0-9_]{20,}", r"ghp_[A-Za-z0-9]{30,}", r"sk-ant-[A-Za-z0-9-]{10,}",
           r"AKIA[0-9A-Z]{16}", r"-----BEGIN [A-Z ]*PRIVATE KEY-----", r"/Users/[a-z][a-z0-9_-]+/"]
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
EMAIL_OK = re.compile(r"(@users\.noreply\.github\.com|@retail\.example|@example\.com|@databricks\.com)$")
for p in ALL_TEXT:
    text = p.read_text(encoding="utf-8", errors="replace")
    for pat in SECRETS:
        check(not re.search(pat, text), f"wording: {p.relative_to(ROOT)} matches secret/private pattern {pat}")
    for m in EMAIL.findall(text):
        check(bool(EMAIL_OK.search(m)), f"wording: {p.relative_to(ROOT)} contains a non-example email address")

# ---------- code ----------
node = shutil.which("node")
if node:
    for page in PAGES:
        for i, body in enumerate(re.findall(r"<script>(.*?)</script>", read(page), re.S)):
            with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as tf:
                tf.write(body)
            r = subprocess.run([node, "--check", tf.name], capture_output=True, text=True)
            check(r.returncode == 0, f"code: {page} script {i} does not parse: {r.stderr.strip()[:200]}")
    r = subprocess.run([node, "--check", str(ROOT / "scripts/export_from_lab.mjs")], capture_output=True, text=True)
    check(r.returncode == 0, "code: scripts/export_from_lab.mjs does not parse")
else:
    print("note: node not found, skipped the JavaScript parse checks")
for nb in ("labs/01_retail_medallion.py", "scripts/build_portal.py", "scripts/build_en.py"):
    try:
        py_compile.compile(str(ROOT / nb), doraise=True, cfile=tempfile.mktemp())
        ok = True
    except py_compile.PyCompileError:
        ok = False
    check(ok, f"code: {nb} does not compile")
if shutil.which("bash"):
    r = subprocess.run(["bash", "-n", str(ROOT / "scripts/publish.sh")], capture_output=True, text=True)
    check(r.returncode == 0, f"code: scripts/publish.sh has a syntax error: {r.stderr.strip()[:200]}")

# ---------- result ----------
if failures:
    print(f"FAIL: {len(failures)} of {checks} checks failed")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print(f"OK: {checks} checks passed")
