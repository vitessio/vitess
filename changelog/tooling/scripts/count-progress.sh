#!/bin/bash

# Progress monitoring script for PR analysis
# Usage: ./count-progress.sh

set -euo pipefail

# Check if we're in an analysis directory
if [ ! -f "all_pr_numbers.txt" ]; then
    echo "❌ Not in an analysis directory. Run from directory containing all_pr_numbers.txt"
    exit 1
fi

# Get counts
TOTAL_PRS=$(wc -l < all_pr_numbers.txt 2>/dev/null || echo "0")
COMPLETED_PRS=$(find . -name "PR*.md" 2>/dev/null | wc -l || echo "0")
REMAINING_PRS=$((TOTAL_PRS - COMPLETED_PRS))

# Calculate percentage
if [ "$TOTAL_PRS" -gt 0 ]; then
    PERCENTAGE=$(( (COMPLETED_PRS * 100) / TOTAL_PRS ))
else
    PERCENTAGE=0
fi

# Display progress
echo "📊 PR Analysis Progress Report"
echo "=========================="
echo "✅ Completed: ${COMPLETED_PRS} PRs"
echo "⏳ Remaining: ${REMAINING_PRS} PRs" 
echo "📈 Total: ${TOTAL_PRS} PRs"
echo "📊 Progress: ${PERCENTAGE}% complete"
echo ""

# Show status breakdown
if [ "$COMPLETED_PRS" -gt 0 ]; then
    echo "📋 Status Breakdown:"
    
    # Count merged vs not merged
    MERGED_COUNT=$(find . -name "PR*.md" -exec grep -L "PR not merged" {} \; 2>/dev/null | wc -l || echo "0")
    NOT_MERGED_COUNT=$(find . -name "PR*.md" -exec grep -l "PR not merged" {} \; 2>/dev/null | wc -l || echo "0")
    
    echo "   🔀 Merged PRs analyzed: ${MERGED_COUNT}"
    echo "   ❌ Not merged PRs: ${NOT_MERGED_COUNT}"
    echo ""
fi

# Time estimates
if [ "$REMAINING_PRS" -gt 0 ]; then
    # Estimate 1 minute per PR on average (batching efficiency)
    REMAINING_MINUTES=$((REMAINING_PRS / 5))  # 5 PRs per batch, ~1 minute per batch
    REMAINING_HOURS=$((REMAINING_MINUTES / 60))
    REMAINING_MINS=$((REMAINING_MINUTES % 60))
    
    echo "⏰ Estimated time remaining:"
    if [ "$REMAINING_HOURS" -gt 0 ]; then
        echo "   ${REMAINING_HOURS}h ${REMAINING_MINS}m"
    else
        echo "   ${REMAINING_MINUTES}m"
    fi
    echo ""
fi

# Next steps
if [ "$REMAINING_PRS" -eq 0 ]; then
    echo "🎉 Analysis complete! Ready to generate final report."
    echo ""
    echo "Next steps:"
    echo "1. Review reports for quality"
    echo "2. Generate comprehensive release documentation" 
    echo "3. Create structured tables for release notes"
else
    echo "📝 Continue analysis with Claude Code agents"
    echo ""
    echo "Next batch example:"
    
    # Show next 5 PRs to analyze
    ANALYZED_PRS=$(find . -name "PR*.md" | sed 's|.*/PR\([0-9]*\)\.md|\1|' | sort -n)
    NEXT_PRS=$(comm -23 <(sort -n all_pr_numbers.txt) <(echo "$ANALYZED_PRS") | head -5 | tr '\n' ', ' | sed 's/,$//')
    
    if [ -n "$NEXT_PRS" ]; then
        echo "   PRs: $NEXT_PRS"
    fi
fi

echo ""