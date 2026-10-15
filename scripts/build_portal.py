#!/usr/bin/env python3
"""Build index.html (the course home page) from data/manifest.json.

    python3 scripts/build_portal.py

Standard library only. Run scripts/export_from_lab.mjs first if the lab data changed.
The page is fully static: no JavaScript, no external assets, works from file://.
"""
import html
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPO_URL = "https://github.com/timwukp/databrick-sagamker-full-stack-self-learning"
man = json.loads((ROOT / "data" / "manifest.json").read_text(encoding="utf-8"))
e = html.escape

STAGES = ["Sources", "Ingest", "Raw (bronze)", "Clean (silver)", "Curated (gold)",
          "Features", "Train and register", "Serve", "Use"]
STRIP = [  # (stage, Databricks, AWS)
    ("Sources", "POS, ERP, promo files", "POS, ERP, promo files"),
    ("Ingest", "Kafka, Auto Loader, Lakeflow Connect", "MSK, Glue jobs, zero-ETL or DMS"),
    ("Raw (bronze)", "Streaming tables (Delta)", "Iceberg in S3 Tables"),
    ("Clean (silver)", "Lakeflow Spark Declarative Pipelines", "Glue ETL or EMR Serverless"),
    ("Curated (gold)", "Materialized views, SQL warehouse, OpenSharing", "Iceberg tables, Athena or Redshift, Lake Formation sharing"),
    ("Features", "Feature Store in UC, Online Feature Store", "SageMaker Feature Store"),
    ("Train and register", "applyInPandas, MLflow 3, Models in UC", "Training jobs, MLflow Apps, Model Registry"),
    ("Serve", "Lakeflow Jobs batch, Model Serving", "Batch Transform, real-time endpoint"),
    ("Use", "Databricks Apps, AI/BI, Genie, Agent Bricks", "API Gateway, Quick, AgentCore"),
]

steps = man["steps"]
files = man["files"]

def stage_of(title):
    for s in STAGES:
        if title.startswith(s + ":"):
            return s
    return "Layers"

# ---- learning path ----
path_rows = []
for st in steps:
    sid, no = st["step"], st["step_no"]
    t_dbx = st["title_databricks"].split(": ", 1)[-1]
    t_aws = st["title_aws"].split(": ", 1)[-1]
    stage = stage_of(st["title_databricks"]) if not sid.startswith("p:") else "Layer"
    if sid.startswith("p:"):
        t_dbx = t_aws = st["title_databricks"].replace("Layer: ", "")
    path_rows.append(
        f'<li><span class="stg">{e(stage)}</span>'
        f'<a class="pl dbx" href="lab/retail-3d.html#step={e(sid)}&amp;plat=dbx">{e(t_dbx)}</a>'
        f'<a class="pl aws" href="lab/retail-3d.html#step={e(sid)}&amp;plat=aws">{e(t_aws)}</a></li>')

# ---- schema catalog ----
cat = []
for st in steps:
    sid, no = st["step"], st["step_no"]
    recs = [f for f in files if f["step_no"] == no]
    rows = []
    roles = sorted({r["role"] for r in recs}, key=lambda r: (r != "before", r))
    for role in roles:
        cell = {}
        for plat in ("databricks", "aws"):
            r = next((x for x in recs if x["role"] == role and x["platform"] == plat), None)
            if r:
                sch = json.loads((ROOT / r["schema_file"]).read_text(encoding="utf-8"))
                cols = ", ".join(f'{c["name"]} {c["type"]}' for c in sch["columns"]) or "(no fixed columns)"
                cell[plat] = (f'<span class="tn">{e(r["name"])}</span>'
                              f'<span class="tc">{r["columns"]} columns, {r["rows"]} {"rows" if r["kind"]=="table" else "items" if r["kind"]=="json" else "lines"}</span>'
                              f'<details><summary>Columns</summary><p class="cols">{e(cols)}</p></details>'
                              f'<a href="{e(r["file"])}">data</a> <a href="{e(r["schema_file"])}">schema</a>')
            else:
                cell[plat] = '<span class="muted">same as Databricks</span>'
        label = "Before" if role == "before" else "After" if role == "after" else "After (more)"
        rows.append(f'<tr><td class="role">{label}</td><td>{cell["databricks"]}</td><td>{cell["aws"]}</td></tr>')
    title = st["title_databricks"]
    cat.append(f'<tr class="grp"><td colspan="3"><a href="lab/retail-3d.html#step={e(sid)}&amp;plat=dbx">{no}. {e(title)}</a></td></tr>' + "".join(rows))

strip = "".join(
    f'<li><span class="sn">{i+1}</span><b>{e(s)}</b><span class="d">{e(d)}</span><span class="a">{e(a)}</span></li>'
    for i, (s, d, a) in enumerate(STRIP))

n_tables = len(files)
page = f"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Databricks × AWS full-stack self-study: global retail forecasting</title>
<meta name="description" content="Self-study course: one retail use case built end to end on Databricks and on AWS, with a 3D interactive diagram, synthetic sample data and schemas for every step.">
<style>
:root{{--paper:#E9EDF0;--card:#fff;--ink:#16212C;--muted:#56636F;--rule:#CBD3DA;--dbx:#C9260F;--aws:#232F3E;--awso:#FF9900;
--sans:"Avenir Next","Segoe UI","Helvetica Neue","PingFang TC","Noto Sans TC",Arial,sans-serif;
--cond:"Avenir Next Condensed","Arial Narrow","Roboto Condensed","Avenir Next","PingFang TC",sans-serif}}
*{{box-sizing:border-box}} html,body{{margin:0}}
body{{background:var(--paper);color:var(--ink);font:15.5px/1.6 var(--sans);-webkit-font-smoothing:antialiased}}
a{{color:inherit}} a:focus-visible,summary:focus-visible{{outline:3px solid var(--ink);outline-offset:2px}}
.skip{{position:absolute;left:-999px}} .skip:focus{{left:12px;top:12px;background:#fff;padding:6px 10px;z-index:9}}
.wrap{{max-width:1180px;margin:0 auto;padding:28px 28px 80px}}
nav.top{{display:flex;flex-wrap:wrap;gap:6px 18px;font-size:14px;margin-bottom:34px}}
nav.top a{{font-weight:600;text-decoration:none;border-bottom:2px solid transparent}} nav.top a:hover{{border-color:var(--ink)}} nav.top a.lang{{margin-left:auto}}
h1{{font:700 clamp(34px,5vw,58px)/1 var(--cond);letter-spacing:-.01em;margin:0 0 14px;max-width:16ch}}
.lede{{font-size:18px;max-width:64ch;color:#2E3A46;margin:0 0 22px}}
.cta{{display:flex;flex-wrap:wrap;gap:10px;margin:0 0 30px}}
.btn{{display:inline-block;font:600 15px var(--sans);text-decoration:none;padding:11px 18px;border-radius:999px;border:1px solid var(--ink)}}
.btn.main{{background:var(--ink);color:#fff}}
h2{{font:700 32px/1.05 var(--cond);margin:64px 0 10px}}
h3{{font:600 17px/1.3 var(--sans);margin:22px 0 8px}}
p{{max-width:76ch}} .muted{{color:var(--muted)}}
ol.strip{{list-style:none;margin:0;padding:0;display:grid;grid-template-columns:repeat(9,minmax(0,1fr));gap:6px}}
ol.strip li{{background:#fff;border:1px solid var(--rule);border-radius:8px;padding:10px 9px 11px;display:flex;flex-direction:column;gap:6px;font-size:12.5px;line-height:1.35}}
ol.strip .sn{{font:700 13px var(--cond);color:var(--muted)}} ol.strip b{{font:700 15px/1.1 var(--cond)}}
ol.strip .d{{border-left:3px solid var(--dbx);padding-left:6px}} ol.strip .a{{border-left:3px solid var(--awso);padding-left:6px}}
.key{{display:flex;gap:18px;font-size:13px;color:var(--muted);margin:10px 0 0}} .key i{{display:inline-block;width:12px;height:12px;border-radius:2px;margin-right:6px;vertical-align:-1px}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid var(--rule);border-radius:8px;overflow:hidden;font-size:14px}}
th,td{{text-align:left;vertical-align:top;padding:10px 12px;border-bottom:1px solid var(--rule)}}
th{{font:600 13px var(--sans);color:var(--muted);background:#F5F7F8}}
ol.path{{list-style:none;counter-reset:p;margin:0;padding:0;display:grid;gap:6px}}
ol.path li{{counter-increment:p;display:grid;grid-template-columns:34px 150px minmax(0,1fr) minmax(0,1fr);gap:10px;align-items:center;background:#fff;border:1px solid var(--rule);border-radius:8px;padding:8px 12px}}
ol.path li::before{{content:counter(p);font:700 15px var(--cond);color:var(--muted)}}
ol.path .stg{{font-size:12.5px;color:var(--muted)}}
a.pl{{text-decoration:none;font-weight:600;font-size:14px;padding:4px 0 4px 10px;border-left:3px solid}} a.pl:hover{{text-decoration:underline}}
a.pl.dbx{{border-color:var(--dbx)}} a.pl.aws{{border-color:var(--awso)}}
.pathhead{{display:grid;grid-template-columns:34px 150px minmax(0,1fr) minmax(0,1fr);gap:10px;font-size:13px;color:var(--muted);padding:0 12px 6px}}
table.cat td{{font-size:13.5px}} table.cat .tn{{display:block;font-weight:600}} table.cat .tc{{display:block;color:var(--muted);font-size:12.5px}}
table.cat .role{{width:96px;color:var(--muted);font-size:13px}} table.cat tr.grp td{{background:#F5F7F8;font:600 14px var(--sans)}}
table.cat a{{font-size:13px;margin-right:8px}} details summary{{cursor:pointer;font-size:12.5px;color:var(--muted)}} p.cols{{font-size:12.5px;margin:4px 0;color:#33404C}}
.cards{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}}
.card{{background:#fff;border:1px solid var(--rule);border-radius:10px;padding:16px 18px}}
.card h3{{margin-top:0}} .card p{{font-size:14px;margin:6px 0}}
pre{{background:#101B26;color:#E6EDF3;border-radius:8px;padding:12px 14px;overflow:auto;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}}
footer{{margin-top:70px;padding-top:18px;border-top:1px solid var(--rule);font-size:13.5px;color:var(--muted)}}
@media (max-width:980px){{ol.strip{{grid-template-columns:repeat(3,minmax(0,1fr))}} .cards{{grid-template-columns:1fr}}
 ol.path li,.pathhead{{grid-template-columns:26px minmax(0,1fr)}} ol.path .stg{{grid-column:2}} ol.path a.pl{{grid-column:2}} .pathhead span:nth-child(n+3){{display:none}}}}
@media (max-width:560px){{.wrap{{padding:18px 14px 60px}} ol.strip{{grid-template-columns:repeat(2,minmax(0,1fr))}} th,td{{padding:8px}}}}
</style>
</head>
<body>
<a class="skip" href="#main">Skip to content</a>
<div class="wrap">
<nav class="top" aria-label="Sections">
  <a href="lab/retail-3d.html">3D lab</a><a href="#use-case">Use case</a><a href="#path">Learning path</a>
  <a href="#catalog">Data and schemas</a><a href="#hands-on">Hands-on</a><a href="#docs">Docs</a><a href="{REPO_URL}">GitHub</a><a class="lang" href="en/" hreflang="en">English</a>
</nav>
<main id="main">
<h1>Databricks × AWS, one use case end to end</h1>
<p class="lede">自學 Databricks 最快的方法之一：拿一個真實的零售場景，從門店 POS 的原始數據一路做到庫存與銷售預測，再用 AWS（SageMaker 與相關服務）做同一件事，逐格比較。每一步都有 3D 互動圖、處理前 / 處理後的合成數據、schema 和官方文件連結。</p>
<div class="cta">
  <a class="btn main" href="lab/retail-3d.html">Open the 3D lab</a>
  <a class="btn" href="lab/retail-3d.html#step=pos&amp;plat=dbx">Walk the data, step 1</a>
  <a class="btn" href="#catalog">Browse {n_tables} sample tables</a>
</div>

<ol class="strip" aria-label="The nine stages, Databricks and AWS">{strip}</ol>
<p class="key"><span><i style="background:var(--dbx)"></i>Databricks</span><span><i style="background:var(--awso)"></i>AWS</span></p>

<h2 id="use-case">The use case</h2>
<p>一家在東南亞有數千家門店的零售商，想同時減少缺貨和庫存積壓。這其實是兩個預測，跑在兩種時鐘上。</p>
<table>
<thead><tr><th></th><th>倉庫庫存預測</th><th>門店銷售預測</th></tr></thead>
<tbody>
<tr><td>粒度</td><td>倉庫 × SKU × 日</td><td>門店 × SKU × 日</td></tr>
<tr><td>預測期與頻率</td><td>未來 30 天，每晚批次</td><td>今天剩下的時間，日內即時</td></tr>
<tr><td>輸出</td><td>建議採購單，送回 ERP 等人核准</td><td>給店經理的動作：「中午前從後倉補 60 個」</td></tr>
<tr><td>服務方式</td><td>batch inference 寫成一張表</td><td>real-time endpoint</td></tr>
</tbody></table>
<h3>合成數據裡的世界</h3>
<p>三家店（新加坡 SG-ORCH-001、吉隆坡 MY-KUL-014、曼谷 TH-BKK-007）、兩個倉庫（DC-SG-01、DC-TH-02）、三個 SKU，營業日 2026-10-09（週五），SKU-20871 在 SG、MY 打八折。原始數據刻意埋了重複事件、晚到事件、新欄位、壞資料、巢狀商品、當地時間字串、補零的 SAP 物料號——每一步處理掉一部分。所有數字前後對得上，例如最後的採購建議 1104 = 46 箱 × 24。</p>
<p><a href="{REPO_URL}/blob/main/docs/use-case.md">Read the full use case</a></p>

<h2 id="path">Learning path</h2>
<p>照順序走一遍。每一步有兩個連結：在 3D lab 打開 Databricks 版或 AWS 版，直接跳到那一步的「處理前 → 處理後」數據。</p>
<div class="pathhead" aria-hidden="true"><span></span><span>Stage</span><span>Databricks</span><span>AWS</span></div>
<ol class="path">{"".join(path_rows)}</ol>

<h2 id="catalog">Sample data and schemas</h2>
<p>每一步、每個平台的樣本表都可以下載（CSV / JSON / 文字檔）和看 schema（JSON）。這些檔案由 <code>scripts/export_from_lab.mjs</code> 從 3D lab 匯出，和網站上看到的是同一份。索引在 <a href="data/manifest.json">data/manifest.json</a>；Databricks Free Edition 練習用的原始檔在 <a href="data/raw/pos_events.jsonl">data/raw/</a>。</p>
<table class="cat">
<thead><tr><th>Role</th><th>Databricks</th><th>AWS</th></tr></thead>
<tbody>{"".join(cat)}</tbody></table>

<h2 id="hands-on">Hands-on</h2>
<div class="cards">
  <div class="card"><h3>1. Run the medallion lab</h3><p>在 Databricks Free Edition 匯入 <a href="{REPO_URL}/blob/main/labs/01_retail_medallion.py">labs/01_retail_medallion.py</a>，用同一份 POS 事件親手跑 Auto Loader → bronze → silver → gold，看到重複、壞資料、晚到事件如何被處理。</p></div>
  <div class="card"><h3>2. Compare one step at a time</h3><p>在 3D lab 選一步，切換 Databricks / AWS，對照 schema 的差異：Delta 與 Iceberg 的型別、Feature Store 的 record identifier、SCD2 與 DMS 的 Op 欄。</p></div>
  <div class="card"><h3>3. Explain it in 90 seconds</h3><p>用 <a href="{REPO_URL}/blob/main/docs/talk-track.md">talk track</a> 把整條管線講一遍，再對照 <a href="{REPO_URL}/blob/main/docs/common-mistakes.md">常見的錯誤說法</a> 檢查自己。</p></div>
</div>
<h3>Run the site locally</h3>
<pre>python3 -m http.server 8000 --bind 127.0.0.1
# open http://localhost:8000/</pre>
<p class="muted">也可以直接雙擊 index.html 或 lab/retail-3d.html：沒有外部資源，離線可用。</p>

<h2 id="docs">Docs</h2>
<table>
<tbody>
<tr><td><a href="{REPO_URL}/blob/main/docs/use-case.md">Use case</a></td><td>業務問題、合成數據的世界、刻意埋的問題、數字如何對帳</td></tr>
<tr><td><a href="{REPO_URL}/blob/main/docs/data-walkthrough.md">Data walkthrough</a></td><td>每一步 × 兩個平台的處理前 / 處理後資料、schema、說明與程式碼（文字版）</td></tr>
<tr><td><a href="{REPO_URL}/blob/main/docs/databricks-vs-aws-mapping.md">Component map</a></td><td>每一格的 Databricks 與 AWS 元件、官方文件連結、學習重點</td></tr>
<tr><td><a href="{REPO_URL}/blob/main/docs/common-mistakes.md">Common mistakes</a></td><td>過期名稱、錯誤數據流、過度承諾，以及正確說法</td></tr>
<tr><td><a href="{REPO_URL}/blob/main/docs/talk-track.md">Talk track</a></td><td>90 秒英文講解，附 AWS 對照</td></tr>
<tr><td><a href="{REPO_URL}/blob/main/docs/sources.md">Sources</a></td><td>逐頁查證過的官方文件（2026-10-10、2026-10-11）</td></tr>
</tbody></table>
</main>
<footer>
<p>Independent self-study material, not affiliated with or endorsed by Databricks or Amazon Web Services. Product names and behaviour were checked against official documentation in October 2026; the sample data is synthetic. Verified on every change by <code>python3 verify.py</code>.</p>
</footer>
</div>
</body>
</html>
"""
(ROOT / "index.html").write_text(page, encoding="utf-8")
print(f"index.html written: {len(steps)} steps, {n_tables} files in the catalog")
