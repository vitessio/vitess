#!/bin/bash

# Automated PR Analysis Script for Vitess Releases
# Usage: ./analyze-milestone.sh <milestone-id>
# Example: ./analyze-milestone.sh 85

set -euo pipefail

# Check arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <milestone-id>"
    echo "Example: $0 85  # for v23 milestone"
    echo "Example: $0 86  # for v24 milestone"
    exit 1
fi

MILESTONE_ID="$1"
ORG="vitessio"  
REPO="vitess"

echo "🚀 Starting Vitess PR analysis for milestone ${MILESTONE_ID}"

# Check prerequisites
command -v gh >/dev/null 2>&1 || { echo "❌ GitHub CLI (gh) is required but not installed."; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "❌ GitHub CLI not authenticated. Run: gh auth login"; exit 1; }

# Create analysis directory
ANALYSIS_DIR="milestone-${MILESTONE_ID}-analysis"
mkdir -p "$ANALYSIS_DIR"
cd "$ANALYSIS_DIR"

echo "📁 Created analysis directory: $ANALYSIS_DIR"

# Fetch all PR numbers from milestone  
echo "📥 Fetching PR numbers from milestone ${MILESTONE_ID}..."
gh api "repos/${ORG}/${REPO}/issues?milestone=${MILESTONE_ID}&state=all" --paginate --jq '.[].number' > all_pr_numbers.txt

TOTAL_PRS=$(wc -l < all_pr_numbers.txt)
echo "✅ Found ${TOTAL_PRS} PRs to analyze"

# Save milestone info
echo "Milestone: ${MILESTONE_ID}" > analysis_metadata.txt
echo "Repository: ${ORG}/${REPO}" >> analysis_metadata.txt  
echo "Total PRs: ${TOTAL_PRS}" >> analysis_metadata.txt
echo "Started: $(date)" >> analysis_metadata.txt

echo ""
echo "📋 Analysis setup complete!"
echo "📊 Total PRs to analyze: ${TOTAL_PRS}"
echo "⏱️  Expected time: 4-6 hours"
echo "💰 Expected cost: \$40-50"
echo ""
echo "🔗 Vitess Milestone: https://github.com/vitessio/vitess/milestone/${MILESTONE_ID}?closed=1"
echo ""
echo "Next steps:"
echo "1. Use Claude Code to launch pr-flag-metric-tracker agents in batches of 5 PRs"
echo "2. Monitor progress with: ../scripts/count-progress.sh"
echo "3. Use this prompt template for Claude Code:"
echo ""
echo "Analyze these 5 PRs in batch. For each:"
echo "1. Check merge: gh pr view https://github.com/vitessio/vitess/pull/XXXX --json state,mergedAt"
echo "2. If NOT merged: Create PRXXXX.md with just 'PR not merged'"
echo "3. If MERGED: Create full analysis focusing on public-facing changes"
echo ""
echo "PRs: [first 5 numbers from all_pr_numbers.txt]"
echo ""