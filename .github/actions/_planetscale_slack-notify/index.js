const core = require("@actions/core");
const { context, GitHub } = require("@actions/github");
const { IncomingWebhook } = require("@slack/webhook");

process.on("unhandledRejection", handleError);
main().catch(handleError);

async function main() {
  const run_id = process.env.GITHUB_RUN_ID;
  const github = new GitHub(process.env.GITHUB_TOKEN);

  const { data: wf_run } = await github.actions.getWorkflowRun({
    owner: context.repo.owner,
    repo: context.repo.repo,
    run_id: run_id,
  });
  debug(JSON.stringify(wf_run, undefined, 2));

  const { data: wf_jobs } = await github.actions.listJobsForWorkflowRun({
    owner: context.repo.owner,
    repo: context.repo.repo,
    run_id: run_id,
  });
  debug(JSON.stringify(wf_jobs, undefined, 2));

  var jobs = [];
  var is_wf_success = true;
  var is_wf_failure = false;
  for (var j of wf_jobs.jobs) {
    // ignore the current job running this script
    if (j.status != "completed") {
      continue;
    }
    if (j.conclusion != "success") {
      is_wf_success = false;
    }
    if (j.conclusion == "failure") {
      is_wf_failure = true;
    }
    jobs.push({
      type: "mrkdwn",
      text: jobTemplate(j),
    });
  }

  var workflow_status = "w_cancelled";
  if (is_wf_success) {
    workflow_status = "w_success";
  } else if (is_wf_failure) {
    workflow_status = "w_failure";
  }

  var blocks = [
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: titleTemplate(context, wf_run),
      },
    },
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: workflowTemplate(context, wf_run, workflow_status),
      },
    },
    {
      type: "divider",
    },
  ];

  // Slack only allows 10 fields per block
  const num_blocks = Math.ceil(jobs.length / 10);
  for (var i = 0; i <= num_blocks - 1; i++) {
    blocks.push({
      type: "section",
      fields: jobs.slice(i * 10, (i + 1) * 10),
    });
  }

  blocks.push(
    {
      type: "divider",
    },
    {
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: jobsSummaryTemplate(wf_jobs.jobs),
        },
      ],
    }
  );

  const slack_msg = { blocks: blocks };
  debug(JSON.stringify(slack_msg, undefined, 2));

  // Use Slack SDK to send message
  const webhook = new IncomingWebhook(process.env.SLACK_WEBHOOK_URL);
  await webhook.send(slack_msg);
}

function titleTemplate(context, wf_run) {
  const upperRef = context.ref.toUpperCase();
  var ref = context.ref;
  if (upperRef.startsWith("REFS/HEADS/")) {
    ref = context.ref.substring("refs/heads/".length);
  } else if (upperRef.startsWith("REFS/PULL/")) {
    ref = context.ref.substring("refs/pull/".length);
  } else if (upperRef.startsWith("REFS/")) {
    ref = context.ref.substring("refs/".length);
  }

  return `
<${wf_run.repository.html_url}|*${context.repo.owner}/${context.repo.repo}*>
from *${ref}@${context.sha.substr(0, 8)}*
`.trim();
}
function jobTemplate(job) {
  const completed_in = dateDiff(
    new Date(job.started_at),
    new Date(job.completed_at)
  );

  return `
${statusIcon(job.conclusion)} <${job.html_url}|${job.name}>
  \u21b3 completed in ${completed_in}
`.trim();
}

function workflowTemplate(context, wf_run, workflow_status) {
  const run_number = process.env.GITHUB_RUN_NUMBER;
  const completed_in = dateDiff(
    new Date(wf_run.created_at),
    new Date(wf_run.updated_at)
  );

  var for_pr = "";
  if (wf_run.pull_requests.length > 0) {
    const pr = wf_run.pull_requests[0];
    // For some reason html_url isn't on on the pull_request object
    const pr_url = `${wf_run.repository.html_url}/pull/${pr.number}`;
    for_pr = ` for <${pr_url}|#${pr.number}>`;
  }

  var wf_conclusion = "cancelled after";
  if (workflow_status === "w_success") {
    wf_conclusion = "succeeded in";
  } else if (workflow_status === "w_failure") {
    wf_conclusion = "failed in";
  }

  return `
${statusIcon(workflow_status)} *${context.workflow}*${for_pr}
Workflow run <${
    wf_run.html_url
  }|#${run_number}> ${wf_conclusion} ${completed_in}
`.trim();
}

function jobsSummaryTemplate(jobs) {
  var num_succeeded = 0;
  for (var j of jobs) {
    if (j.status != "completed") {
      continue;
    }
    if (j.conclusion == "success") {
      num_succeeded++;
    }
  }
  return `${num_succeeded}/${jobs.length - 1} successful checks`;
}

function dateDiff(start, end) {
  var duration = end - start;
  var delta = duration / 1000;
  var days = Math.floor(delta / 86400);
  delta -= days * 86400;
  var hours = Math.floor(delta / 3600) % 24;
  delta -= hours * 3600;
  var minutes = Math.floor(delta / 60) % 60;
  delta -= minutes * 60;
  var seconds = Math.floor(delta % 60);
  var format_func = function (v, text, check) {
    if (v <= 0 && check) {
      return "";
    } else {
      return v + text;
    }
  };
  return (
    format_func(days, "d", true) +
    format_func(hours, "h", true) +
    format_func(minutes, "m", true) +
    format_func(seconds, "s", false)
  );
}

function statusIcon(s) {
  switch (s) {
    case "w_success":
      return ":heavy_check_mark:";
    case "w_failure":
      return ":heavy_multiplication_x:";
    case "w_cancelled":
      return ":octagonal_sign:";
    case "success":
      return "\u2713";
    case "failure":
      return "\u2717";
    default:
      return "\u20e0";
  }
}

function debug(str) {
  // GitHub Actions replaces curly braces with splats
  core.debug(str.replace(/{/g, "((").replace(/}/g, "))"));
}

function handleError(err) {
  console.error(err);

  if (err && err.message) {
    core.setFailed(err.message);
  } else {
    core.setFailed(`Unhandled error: ${err}`);
  }
}
