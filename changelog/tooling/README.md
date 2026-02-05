# Vitess Release Documentation Tooling

This directory contains automated tools and methodologies for analyzing Vitess pull requests and generating comprehensive release documentation.

## Contents

### Core Documentation
- **`automated-pr-analysis-guide.md`** - Complete methodology for analyzing hundreds of PRs efficiently using specialized agents
- **`pr-flag-metric-tracker.md`** - Agent definition file for automated PR analysis (copy to `.claude/agents/`)

### Examples
- **`examples/sample-pr-report.md`** - Example of individual PR analysis output
- **`examples/sample-final-report.md`** - Example of comprehensive release documentation
- **`examples/milestone-url-examples.md`** - How to find GitHub milestone URLs

### Scripts
- **`scripts/analyze-milestone.sh`** - Automated setup script for milestone analysis
- **`scripts/count-progress.sh`** - Progress monitoring during analysis

### Templates
- **`templates/release-notes-template.md`** - Template for final release documentation

## Quick Start

1. **Setup**: Copy `pr-flag-metric-tracker.md` to your `.claude/agents/` directory
2. **Find milestone**: Use `examples/milestone-url-examples.md` to locate your milestone URL  
3. **Run setup**: Execute `scripts/analyze-milestone.sh <milestone-id>` (e.g., `85` for v23)
4. **Monitor progress**: Use `scripts/count-progress.sh` to track completion
5. **Generate report**: Follow the guide in `automated-pr-analysis-guide.md`

**Expected time**: 4-6 hours for ~276 PRs  
**Expected cost**: $40-50 using Claude Code

## Prerequisites

- GitHub CLI (`gh`) authenticated
- Claude Code with agent support
- Repository access permissions
- ~5GB free disk space for reports

## Output

- Individual PR reports (`PR{number}.md`) 
- Comprehensive API changes report
- Structured tables for release notes