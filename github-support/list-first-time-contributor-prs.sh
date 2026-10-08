#!/usr/bin/env bash
# Temporary test script for the first-time-contributor-prs workflow logic.
# Lists open PRs from first-time contributors that have no CI runs on their current head SHA.
#
# Usage: ./github-support/list-first-time-contributor-prs.sh
#
# To override the ignored repos list set the IGNORED_REPOS env var to a space-separated
# list of repository names (without the org prefix), e.g.:
#   IGNORED_REPOS="debezium-design-documents debezium-connector-db2" ./github-support/list-first-time-contributor-prs.sh

set -euo pipefail

# Default matches the workflow-level env var
IGNORED_REPOS="${IGNORED_REPOS:-debezium-design-documents}"

PR_LIST=""

# Build ignored repos array from the IGNORED_REPOS env var (space-separated names)
IGNORED_REPOS_ARRAY=()
for repo in $IGNORED_REPOS; do
  IGNORED_REPOS_ARRAY+=("debezium/${repo}")
done
echo "Ignoring repositories: ${IGNORED_REPOS_ARRAY[*]:-none}"

is_ignored() {
  local repo="$1"
  for ignored in "${IGNORED_REPOS_ARRAY[@]:-}"; do
    [ "$repo" = "$ignored" ] && return 0
  done
  return 1
}

echo "Fetching non-archived public repositories of the debezium organization..."

REPOS=$(gh api --paginate "orgs/debezium/repos?type=public" \
  --jq '.[] | select(.archived == false) | .full_name')

for REPO in $REPOS; do
  if is_ignored "$REPO"; then
    echo "Skipping ignored repository: $REPO"
    continue
  fi
  echo "Checking repository: $REPO"

  PRS=$(gh api --paginate "repos/${REPO}/pulls?state=open&per_page=100" \
    --jq '.[] | select(.author_association == "FIRST_TIME_CONTRIBUTOR") | {number: .number, url: .html_url, title: .title, head_sha: .head.sha}' \
    2>/dev/null || true)

  if [ -z "$PRS" ]; then
    continue
  fi

  while IFS= read -r PR_JSON; do
    [ -z "$PR_JSON" ] && continue

    PR_NUMBER=$(echo "$PR_JSON" | jq -r '.number')
    PR_URL=$(echo "$PR_JSON" | jq -r '.url')
    PR_TITLE=$(echo "$PR_JSON" | jq -r '.title')
    HEAD_SHA=$(echo "$PR_JSON" | jq -r '.head_sha')

    # Check if any workflow run for this commit is waiting for approval
    ACTION_REQUIRED=$(gh api "repos/${REPO}/actions/runs?event=pull_request&head_sha=${HEAD_SHA}" \
      --jq '[.workflow_runs[] | select(.conclusion == "action_required")] | length' \
      2>/dev/null || echo "0")

    if [ "$ACTION_REQUIRED" != "0" ]; then
      echo "  -> PR #${PR_NUMBER} in ${REPO} is waiting for CI approval: ${PR_URL}"
      PR_LIST="${PR_LIST}\n- [${REPO} #${PR_NUMBER}](${PR_URL}): ${PR_TITLE}"
    else
      echo "  -> PR #${PR_NUMBER} in ${REPO} has CI running or completed - skipping"
    fi
  done < <(echo "$PRS" | jq -c '.')
done

echo ""
echo "========================================"
if [ -n "$PR_LIST" ]; then
  echo "First-time contributor PRs awaiting CI:"
  echo -e "$PR_LIST"
else
  echo "No first-time contributor PRs awaiting CI found."
fi
echo "========================================"
