# Code Review Instructions

## Priority
Only comment on issues that affect correctness, security, or performance.
Do NOT comment on: style preferences, minor naming conventions, formatting, or
issues already enforced by our linter/CI pipeline.

## Confidence threshold
Only leave a comment when you have HIGH CONFIDENCE (>80%) that a real problem exists.
Do not flag potential issues speculatively.

## Severity filter
Skip LOW severity issues entirely. Focus on HIGH and CRITICAL issues only.

## CI context
Do not flag issues that are caught by our automated CI/CD pipeline (linting, tests, type checks).
