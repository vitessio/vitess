# Slack Notify

## Setup

Add this as this job to the workflow you want notifications from. Make sure it's the last job in the pipeline.

```yaml
slack-notify:
  name: Send Slack Notifiication
  needs:
    - last_job
  if: always()
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v2
    - uses: ./.github/actions/slack-notify
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
```

### Secrets

- `GITHUB_TOKEN` is provided by [GitHub Actions](https://help.github.com/en/actions/configuring-and-managing-workflows/authenticating-with-the-github_token)
- `SLACK_WEBHOOK_URL` comes from setting up an [Incoming Webhook App](https://api.slack.com/messaging/webhooks)

## Build

```console
$ npm install
$ npm run package
```

## Message Layout

Use the [Slack Block Kit Builder](https://api.slack.com/tools/block-kit-builder)

Here are JSON of each message that can be pasted into the builder: [message example](message_blocks.json)
