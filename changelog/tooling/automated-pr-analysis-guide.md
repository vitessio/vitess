# Automated PR Analysis for Release Documentation - Methodology & Approach

## Overview

This document describes an efficient methodology for analyzing large numbers of pull requests (PRs) to generate comprehensive release
documentation focusing on public-facing API changes, flag modifications, and breaking changes.

_Key Innovation: Specialized Agent Architecture_

### Core Concept

Instead of manually reviewing hundreds of PRs, we used specialized pr-flag-metric-tracker agents that can work in parallel to analyze
PRs systematically for specific types of changes. The agent description can be found in the file `pr-flag-metric-tracker.md`. This can be copied into the `.claude/agents` directory.

### Agent Capabilities

- Automatically fetch PR content using GitHub CLI (gh pr view, gh pr diff, gh api)
- Parse code changes for flag additions/deletions/modifications
- Identify metric changes (Prometheus counters, gauges, etc.)
- Detect API changes (gRPC/HTTP endpoints)
- Find parser modifications (SQL syntax changes)
- Spot query planning behavior changes
- Generate standardized reports

## Methodology: Three-Phase Approach

### Phase 1: Bulk PR Discovery

#### Get all PRs from milestone
```
gh api 'repos/org/repo/issues?milestone=X&state=all' --paginate --jq '.[].number'
```

### Phase 2: Parallel Analysis with Merge Filtering

Key Innovation: Check merge status BEFORE analysis to avoid wasting time on unmerged PRs

#### Check if PR was actually merged (not just closed)
```
gh pr view PR_URL --json state,mergedAt
```

Decision Tree:
- If mergedAt is null → Create simple "PR not merged" file
- If mergedAt has date → Perform full analysis

### Phase 3: Batched Agent Deployment

Deploy agents in batches of 5-10 PRs simultaneously for maximum parallelization while avoiding rate limits.

#### Template Standardization

By using a template, we can guide the agents to no be wordy and write a lot of unneccesary info to the reports that would then just take time to read and ignore. The agent profile describes a specific template to use.

### Key Principles

- Focus only on user-facing changes
- No PR metadata or implementation details
- Standardized sections for easy parsing
- "No public changes" for PRs with only internal modifications

## Efficiency Optimizations

1. Batch Processing

- Process 5-10 PRs per agent call
- Parallel execution across multiple agents
- Reduces API calls and context switching

2. Smart Filtering

- Merge status check first - eliminates ~30% of PRs immediately
- Public-facing focus - skip internal refactoring and test-only changes
- Template enforcement - consistent output format for easy aggregation

3. Pre-approved Command Strategy

Ensure agents only use pre-approved GitHub commands to avoid permission prompts:
- gh pr view (approved)
- gh pr diff (approved)
- gh api (approved)

4. Progressive Refinement

- Start with checking for closed and merged PRs
- Only analyze merged PRs
- Avoid re-work through systematic tracking

### Scalability Lessons

What Worked Well

1. Agent specialization - Single-purpose agents are more reliable than general-purpose
2. Parallel execution - 5x faster than sequential analysis
3. Standardized templates - Easy to aggregate and parse results
4. Merge filtering - Eliminates ~30% of work upfront
5. Batching - Reduces overhead and improves throughput

### What to Avoid

- Verbose reports with implementation details
- Analyzing non-merged PRs
- Sequential processing
- Inconsistent report formats
- Re-analyzing already completed work

##  Output Processing

### Individual PR Reports

Each PR gets a focused report following the standard template, making it easy to:
- Scan for breaking changes
- Identify new features
- Track deprecations
- Generate migration guides

### Aggregate Reporting

Parse all individual reports to create comprehensive release documentation with:
- Structured tables by change category
- Component-wise organization
- Breaking change highlights
- Migration timelines

## Replication Instructions

### Prerequisites

- **GitHub CLI (`gh`)**: Authenticated and configured (`gh auth login`)
- **Claude Code**: With agent support enabled
- **Repository access**: Read permissions to target repository
- **Agent setup**: Copy `pr-flag-metric-tracker.md` to your `.claude/agents/` directory
- **Disk space**: ~5GB for storing individual PR reports
- **Time estimate**: 4-6 hours for 276 PRs
- **Cost estimate**: $40-50 in Claude API usage

### Step-by-Step Process

#### 1. Initial Setup
```bash
# Authenticate GitHub CLI if not already done
gh auth login

# Copy agent definition to Claude agents directory  
cp pr-flag-metric-tracker.md ~/.claude/agents/

# Create working directory
mkdir release-analysis && cd release-analysis
```

#### 2. Fetch Milestone PRs
```bash
# Get milestone ID from GitHub UI, then fetch all PR numbers
gh api 'repos/vitessio/vitess/issues?milestone=MILESTONE_ID&state=all' --paginate --jq '.[].number' > all_pr_numbers.txt

# Verify count
echo "Total PRs to analyze: $(wc -l < all_pr_numbers.txt)"
```

#### 3. Launch Batched Analysis
Launch agents in batches of 5 PRs at a time using this prompt template:

```
Analyze these 5 PRs in batch. For each:
1. Check merge: gh pr view https://github.com/vitessio/vitess/pull/XXXX --json state,mergedAt
2. If NOT merged: Create PRXXXX.md with just "PR not merged"
3. If MERGED: Create full analysis with template focusing on public-facing changes

PRs: 18520, 18521, 18522, 18523, 18524

Use ONLY: gh pr view, gh pr diff, gh api, Edit tool. Focus on flags, metrics, APIs, parser changes, query planning.
```

#### 4. Monitor Progress
```bash
# Check completion status
ls PR*.md | wc -l
echo "Progress: $(ls PR*.md | wc -l)/$(wc -l < all_pr_numbers.txt)"
```

#### 5. Generate Final Report
Once all PRs analyzed, create comprehensive release documentation by parsing individual reports into structured tables.

## Troubleshooting

### Common Issues

**Permission Errors with GitHub CLI**:
- Ensure `gh pr view`, `gh pr diff`, `gh api` are pre-approved in Claude Code
- Check GitHub token permissions

**Agent Rate Limiting**:
- Reduce batch size from 5 to 3 PRs
- Add delays between batches if needed

**Inconsistent Report Formats**:
- Emphasize template adherence in agent prompts
- Review and correct agent instructions

**Missing PRs**:
- Some PR numbers may not exist (normal in GitHub)
- Agents will handle gracefully with "PR not found" reports

### Performance Tips

- **Batch size**: 5-10 PRs per agent call is optimal
- **Parallel agents**: Launch multiple agent batches simultaneously  
- **Template enforcement**: Be strict about output format for easier parsing
- **Merge filtering**: Always check merge status first to avoid wasted analysis

## Expected Results

**Time Performance**:
- **Manual approach**: 2-3 minutes per PR = 9-14 hours for 276 PRs
- **Automated approach**: 4-6 hours total including setup
- **Efficiency gain**: 70%+ time reduction

**Output Quality**:
- Standardized format across all reports
- Focus on user-impacting changes only  
- Easy to parse for release documentation
- Comprehensive coverage with no missed PRs