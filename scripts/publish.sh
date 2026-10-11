#!/bin/bash
# Publish this folder to GitHub with the Git Data API (no `git push` needed).
# Follows "使用 GitHub Git Data API 推送程式碼 v2" (2026-08-20):
#   - empty repo: bootstrap with the Contents API, then a full tree, no force
#   - repo with history: new branch + PR (default) or --direct fast-forward of main
#   - blob list is tab-separated, so paths with spaces survive
#   - never uses force=true
#   - author and committer are both set, using the noreply address
#
# Usage (from the repository folder, on a machine where `gh auth login` is done):
#   bash scripts/publish.sh                # first run: creates/bootstraps the repo and publishes main
#   bash scripts/publish.sh --direct       # later: fast-forward main directly
#   bash scripts/publish.sh                # later: open a pull request instead (default)
#   bash scripts/publish.sh --dry-run      # list what would be uploaded, change nothing
#
# Requirements: gh (logged in, token with repo scope), jq, base64, python3.
set -euo pipefail

OWNER="${OWNER:-timwukp}"
REPO="${REPO:-databrick-sagamker-full-stack-self-learning}"
DESCRIPTION="Self-study: one retail forecasting use case end to end on Databricks and AWS, with a 3D interactive diagram, synthetic data and schemas."
MODE="pr"; DRY=0
for a in "$@"; do
  case "$a" in
    --direct) MODE="direct" ;;
    --dry-run) DRY=1 ;;
    *) echo "unknown option: $a" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

say() { printf '\n==> %s\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }; }
need gh; need jq; need base64; need python3

# ---------- 0. local checks ----------
say "Running verify.py"
python3 verify.py

say "Secret scan"
if grep -RInE "github_pat_|ghp_[A-Za-z0-9]{30,}|sk-ant-|AKIA[0-9A-Z]{16}|BEGIN [A-Z ]*PRIVATE KEY" \
     --exclude-dir=.git --exclude=publish.sh --exclude=verify.py . ; then
  echo "Possible secret found above. Fix it before publishing." >&2; exit 1
fi
echo "clean"

# files to publish: everything except git metadata and OS junk
find . -type f ! -path './.git/*' ! -name '.DS_Store' ! -name '*.pyc' ! -path '*/__pycache__/*' -print \
  | sed 's|^\./||' | LC_ALL=C sort > "$WORK/files.txt"
COUNT=$(wc -l < "$WORK/files.txt" | tr -d ' ')
echo "$COUNT files to publish"
if [ "$DRY" = 1 ]; then cat "$WORK/files.txt"; exit 0; fi

gh auth status >/dev/null 2>&1 || { echo "Run: gh auth login" >&2; exit 1; }

# ---------- 1. repo exists? ----------
say "Checking github.com/$OWNER/$REPO"
if ! gh api "repos/$OWNER/$REPO" --silent 2>/dev/null; then
  echo "Repository not found. Creating it as public (GitHub Pages on a free plan needs a public repo)."
  gh repo create "$OWNER/$REPO" --public --description "$DESCRIPTION" >/dev/null
fi
VISIBILITY=$(gh api "repos/$OWNER/$REPO" --jq .visibility)
echo "visibility: $VISIBILITY"

# author identity: noreply address so the commit links to the account without exposing an email
USER_ID=$(gh api user --jq .id)
LOGIN=$(gh api user --jq .login)
NAME=$(gh api user --jq '.name // .login')
EMAIL="${USER_ID}+${LOGIN}@users.noreply.github.com"

# ---------- 2. empty repo? ----------
EMPTY=0
if ! gh api "repos/$OWNER/$REPO/git/refs/heads/main" --silent 2>/dev/null; then
  if [ "$(gh api "repos/$OWNER/$REPO" --jq .size)" = "0" ]; then EMPTY=1; else
    echo "The repo has content but no 'main' branch. Rename its default branch to main first." >&2; exit 1
  fi
fi

if [ "$EMPTY" = 1 ]; then
  say "Empty repo: bootstrapping with the Contents API (Git Data API returns 409 on empty repos)"
  BASE_SHA=$(gh api -X PUT "repos/$OWNER/$REPO/contents/.init" \
      -f message="bootstrap" -f content="$(printf init | base64)" --jq '.commit.sha')
  MODE="initial"
else
  BASE_SHA=$(gh api "repos/$OWNER/$REPO/git/refs/heads/main" --jq .object.sha)
fi
echo "base commit: $BASE_SHA"

# ---------- 3. blobs (tab-separated list) ----------
say "Uploading $COUNT blobs"
: > "$WORK/blob-shas.txt"
while IFS= read -r p; do
  sha=$(base64 < "$p" | tr -d '\n' \
        | jq -Rs '{content: ., encoding: "base64"}' \
        | gh api -X POST "repos/$OWNER/$REPO/git/blobs" --input - --jq .sha)
  mode="100644"; [ -x "$p" ] && mode="100755"
  case "$p" in *.sh) mode="100755" ;; esac
  printf '%s\t%s\t%s\n' "$sha" "$p" "$mode" >> "$WORK/blob-shas.txt"
  printf '  %s\n' "$p"
done < "$WORK/files.txt"

# ---------- 4. tree ----------
TREE_JSON=$(jq -nR '[inputs | split("\t") | {path: .[1], mode: .[2], type: "blob", sha: .[0]}]' < "$WORK/blob-shas.txt")
if [ "$MODE" = "initial" ]; then
  # full tree without base_tree: .init disappears in the release commit
  TREE_SHA=$(jq -n --argjson t "$TREE_JSON" '{tree: $t}' \
             | gh api -X POST "repos/$OWNER/$REPO/git/trees" --input - --jq .sha)
else
  BASE_TREE=$(gh api "repos/$OWNER/$REPO/git/commits/$BASE_SHA" --jq .tree.sha)
  TREE_SHA=$(jq -n --argjson t "$TREE_JSON" --arg base "$BASE_TREE" '{base_tree: $base, tree: $t}' \
             | gh api -X POST "repos/$OWNER/$REPO/git/trees" --input - --jq .sha)
fi

# ---------- 5. commit ----------
if [ "$MODE" = "initial" ]; then
  MSG="Initial release: Databricks × AWS self-study site

3D interactive lab, synthetic sample data and schemas for every step,
docs, a Databricks Free Edition notebook and the site verifier."
else
  MSG="Update the self-study site

Published with scripts/publish.sh ($(date -u +%Y-%m-%dT%H:%MZ))."
fi
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
COMMIT_SHA=$(jq -n --arg msg "$MSG" --arg tree "$TREE_SHA" --arg parent "$BASE_SHA" \
                   --arg name "$NAME" --arg email "$EMAIL" --arg date "$NOW" \
  '{message: $msg, tree: $tree, parents: [$parent],
    author: {name: $name, email: $email, date: $date},
    committer: {name: $name, email: $email, date: $date}}' \
  | gh api -X POST "repos/$OWNER/$REPO/git/commits" --input - --jq .sha)
echo "commit: $COMMIT_SHA"

# ---------- 6. ref (fast-forward only, never force) ----------
if [ "$MODE" = "pr" ]; then
  BRANCH="site/update-$(date -u +%Y%m%d-%H%M)"
  gh api -X POST "repos/$OWNER/$REPO/git/refs" -f ref="refs/heads/$BRANCH" -f sha="$COMMIT_SHA" >/dev/null
  BODY=$(cat <<'EOF'
## Summary
- Site update published with `scripts/publish.sh` (Git Data API, no force).

## Test plan
- [x] `python3 verify.py` passed locally before publishing
EOF
)
  PR_URL=$(jq -n --arg t "Update the self-study site" --arg b "$BODY" --arg h "$BRANCH" --arg base "main" \
             '{title: $t, body: $b, head: $h, base: $base}' \
           | gh api -X POST "repos/$OWNER/$REPO/pulls" --input - --jq .html_url)
  say "Pull request opened: $PR_URL"
  echo "Merge it to update the site."
else
  gh api -X PATCH "repos/$OWNER/$REPO/git/refs/heads/main" -f sha="$COMMIT_SHA" >/dev/null
  say "main now points to $COMMIT_SHA"
fi

# ---------- 7. GitHub Pages: branch main, path / ----------
if [ "$MODE" != "pr" ]; then
  say "GitHub Pages"
  if gh api "repos/$OWNER/$REPO/pages" --silent 2>/dev/null; then
    echo "already enabled"
  elif [ "$VISIBILITY" != "public" ]; then
    echo "The repo is $VISIBILITY. Pages for private repos needs a paid plan; make it public or enable Pages in Settings."
  else
    jq -n '{source: {branch: "main", path: "/"}}' \
      | gh api -X POST "repos/$OWNER/$REPO/pages" --input - >/dev/null && echo "enabled: main /"
  fi
  echo "Site: https://$OWNER.github.io/$REPO/ (first build takes a minute or two)"
fi

# confirm the commit is linked to the account
gh api "repos/$OWNER/$REPO/commits/$COMMIT_SHA" --jq '"author: " + (.author.login // "not linked")' || true
