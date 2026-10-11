#!/usr/bin/env python3
"""Build the English site under en/ from the Chinese pages and i18n/en.json.

    python3 scripts/build_en.py

The Chinese pages stay the source of truth. i18n/en.json maps every Chinese text
segment to English; this script replaces them (longest first), rewrites paths for
the en/ folder, swaps the language switcher, and fails if any Chinese is left.
It also writes docs/en/talk-track.md, docs/en/sources.md and docs/en/common-mistakes.md.
Run scripts/export_from_lab.mjs --lang en afterwards for the two generated English docs.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CJK = re.compile(r"[　-〿㐀-鿿＀-￯]")
REPO = "https://github.com/timwukp/databrick-sagamker-full-stack-self-learning"
SWITCH_ZH = "中文"   # the only Chinese allowed on English pages: the label of the link back

tr = json.loads((ROOT / "i18n" / "en.json").read_text(encoding="utf-8"))
keys = sorted(tr, key=len, reverse=True)


def translate(text, where):
    for k in keys:
        text = text.replace(k, tr[k])
    left = CJK.findall(text.replace(SWITCH_ZH, ""))
    if left:
        sys.exit(f"{where}: untranslated Chinese remains: {''.join(left[:60])}")
    return text


def must_replace(text, old, new, where):
    if old not in text:
        sys.exit(f"{where}: expected snippet not found: {old[:80]}")
    return text.replace(old, new)


# ---------- en/lab/retail-3d.html ----------
lab = (ROOT / "lab" / "retail-3d.html").read_text(encoding="utf-8")
lab = must_replace(lab, '<html lang="zh-Hant">', '<html lang="en">', "lab")
lab = must_replace(lab, '<a class="lang" href="../en/lab/retail-3d.html" hreflang="en">English</a>',
                   f'<a class="lang" href="../../lab/retail-3d.html" hreflang="zh-Hant">{SWITCH_ZH}</a>', "lab")
lab = translate(lab, "en/lab/retail-3d.html")
(ROOT / "en" / "lab").mkdir(parents=True, exist_ok=True)
(ROOT / "en" / "lab" / "retail-3d.html").write_text(lab, encoding="utf-8")

# ---------- en/index.html ----------
home = (ROOT / "index.html").read_text(encoding="utf-8")
home = must_replace(home, '<html lang="zh-Hant">', '<html lang="en">', "index")
home = must_replace(home, '<a class="lang" href="en/" hreflang="en">English</a>',
                    f'<a class="lang" href="../" hreflang="zh-Hant">{SWITCH_ZH}</a>', "index")
home = home.replace('href="data/', 'href="../data/')
home = home.replace(f'{REPO}/blob/main/docs/', f'{REPO}/blob/main/docs/en/')
home = translate(home, "en/index.html")
(ROOT / "en" / "index.html").write_text(home, encoding="utf-8")

# ---------- English docs that are copies or extracts ----------
den = ROOT / "docs" / "en"
den.mkdir(parents=True, exist_ok=True)
talk = (ROOT / "docs" / "talk-track.md").read_text(encoding="utf-8")
talk = talk.replace("](../lab/retail-3d.html#talk)", "](../../en/lab/retail-3d.html#talk)")
(den / "talk-track.md").write_text(translate(talk, "docs/en/talk-track.md"), encoding="utf-8")
src = (ROOT / "docs" / "sources.md").read_text(encoding="utf-8")
(den / "sources.md").write_text(translate(src, "docs/en/sources.md"), encoding="utf-8")

# common mistakes: rebuilt from the English lab page
import html as _html
def strip(x):
    return _html.unescape(re.sub(r"<[^>]+>", "", x)).strip()
fx = re.search(r'<section class="block" id="fixes">(.*?)</section>', lab, re.S).group(1)
title = strip(re.search(r"<h2>(.*?)</h2>", fx, re.S).group(1))
sub = strip(re.search(r'<p class="sub">(.*?)</p>', fx, re.S).group(1))
heads = [strip(h) for h in re.findall(r"<th[^>]*>(.*?)</th>", fx, re.S)]
rows = re.findall(r"<tr><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td></tr>", fx, re.S)
md = [f"# {title}", "", sub, "", "| " + " | ".join(heads) + " |", "|---|---|---|"]
md += ["| " + " | ".join(strip(c).replace("|", "\\|") for c in r) + " |" for r in rows]
(den / "common-mistakes.md").write_text("\n".join(md) + "\n", encoding="utf-8")

print("en/ built: index.html, lab/retail-3d.html; docs/en: talk-track, sources, common-mistakes")
