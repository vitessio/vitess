# Finding Vitess Milestone URLs

## Vitess Release Milestones

### Recent Releases

- **v23 milestone**: https://github.com/vitessio/vitess/milestone/85?closed=1
- **v22 milestone**: https://github.com/vitessio/vitess/milestone/84?closed=1
- **v21 milestone**: https://github.com/vitessio/vitess/milestone/83?closed=1

### Pattern for Future Releases

**Pattern**: `https://github.com/vitessio/vitess/milestone/NUMBER?closed=1`

### How to Find Vitess Milestone URLs

1. **Navigate to**: https://github.com/vitessio/vitess
2. **Click "Issues"** tab
3. **Click "Milestones"** link
4. **Find your milestone**: Look for the release milestone (e.g., "v23.0.0")
5. **Click milestone name**: This opens the milestone page
6. **Add `?closed=1`** to URL to see closed/merged PRs
7. **Copy full URL**: Use this URL with the analysis tools

### Milestone ID vs URL

**You can use either**:
- **Milestone ID**: `85` (for API calls)
- **Milestone URL**: `https://github.com/vitessio/vitess/milestone/85?closed=1` (for web scraping)

### API Equivalent

```bash
# Using milestone ID directly
gh api 'repos/vitessio/vitess/issues?milestone=85&state=all' --paginate --jq '.[].number'
```

### Tips

- **Include `?closed=1`** in URLs to see all PRs (open + closed + merged)
- **Milestone numbers** are sequential integers assigned by GitHub
- **Check milestone status** before starting analysis to ensure it's complete