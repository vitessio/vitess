#!/bin/bash

set -e

echo "🔄 Syncing fork with upstream..."

# Save current branch
CURRENT_BRANCH=$(git branch --show-current)
echo "📍 Current branch: $CURRENT_BRANCH"

# Switch to main
echo "🔀 Switching to main branch..."
git checkout main

# Fetch upstream changes
echo "⬇️  Fetching upstream changes..."
git fetch upstream

# Merge upstream/main
echo "🔗 Merging upstream/main..."
git merge upstream/main

# Push to origin
echo "⬆️  Pushing to origin..."
git push origin main

# Return to original branch if it wasn't main
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "🔙 Returning to branch: $CURRENT_BRANCH"
    git checkout "$CURRENT_BRANCH"
fi

echo "✅ Fork synced successfully!"
