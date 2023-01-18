# Online DDL Scheduler

The DDL scheduler is a control plane that runs on a `PRIMARY` vttablet, as part of the state manager. It is responsible for identifying new migration requests, to choose and execute the next migration, to review running migrations, cleaning up after completion, etc.

This document explains the general logic behind `onlineddl.Executor` and, in particular, the scheduling aspect.

## OnlineDDL & VTTablet state manager

`onlineddl.Executor` runs on `PRIMARY` tablets. It `Open`s when a tablet turns primary, and `Close`s when the tablet changes its type away from `PRIMARY`. It only operates when in the open state.

## General operations

The scheduler:

- Identifies queued migrations
- Picks next migration to run
- Executes a migration
- Follows up on migration progress
- Identifies completion or failure
- Cleans up artifacts
- Identifies stale (rogue) migrations that need to be marked as failed
- Identifies migrations started by another tablet
- Possibly auto-retries migrations

The executor also receives requests from the tablet's query engine/executor to:

- Submit a new migration
- Cancel a migration
- Retry a migration

It also responds on the following API endpoint:

- `/schema-migration/report-status`: called by `gh-ost` and `pt-online-schema-change` to report liveness, completion or failure.

# The scheduler

Breaking down the scheduler logic

## Migration states & transitions

A migration can be in any one of these states:

- `queued`: a migration is submitted
- `ready`: a migration is picked from the queue to run
- `running`: a migration was started. It is periodically tested to be making progress.
- `complete`: a migration completed successfully
- `failed`: a migration failed due to whatever reason. It may have ran for a while, or it may have been marked as `failed` before even running.
- `cancelled`: a _pending_ migration was cancelled

A migration is said to be _pending_ if we expect it to run and complete. Pending migrations are those in `queued`, `ready` and `running` states.

Some possible state transitions are:

- `queued -> ready -> running -> complete`: the ideal flow where everything just works
- `queued -> ready -> running -> failed`: a migration breaks
- `queued -> cancelled`: a migration is cancelled by the user before taken out of queue
- `queued -> ready -> cancelled`: a migration is cancelled by the user before running
- `queued -> ready -> running -> failed`: a running migration is cancelled by the user and forcefully terminated, causing it to enter the `failed` state
- `queued -> ready -> running -> failed -> running`: a failed migration was _retried_
- `queued -> ... cancelled -> queued -> ready -> running`: a cancelled migration was _retried_ (irrespective of whether it was running at time of cancellation)
- `queued -> ready -> cancelled -> queued -> ready -> running -> failed -> running -> failed -> running -> completed`: a combined flow that shows we can retry multiple times

## General logic

The scheduler works by periodically sampling the known migrations. Normally there's a once per minute tick that kicks in a series of checks. You may imagine a state machine that advances once per minute. However, some steps such as:

- Submission of a new migration
- Migration execution start
- Migration execution completion
- Open() state
- Test suite scenario

will kick a burst of additional ticks. This is done to speed up the progress of the state machine. For example, if a new migration is submitted, there's a good chance it will be clear to execute, so an increase in ticks will start the migration within a few seconds rather than one minute later.

By default, Vitess schedules all migrations to run sequentially. Only a single migration is expected to run at any given time. However, there are cases for concurrent execution of migrations, and the user may request concurrent execution via `--allow-concurrent` flag in `ddl_strategy`. Some migrations are eligible to run concurrently, other migrations are eligible to run specific phases concurrently, and some do not allow concurrency. See the user guides for up-to-date information.

## Who runs the migration

Some migrations are executed by the scheduler itself, some by a sub-process, and some implicitly by vreplication, as follows:

- `CREATE TABLE` migrations are executed by the scheduler.
- `DROP TABLE` migrations are executed by the scheduler.
- `ALTER TABLE` migrations depend on `ddl_strategy`:
  - `vitess`/`online`: the scheduler configures, creates and starts a VReplication stream. From that point on, the tablet manager's VReplication logic takes ownership of the execution. The scheduler periodically checks progress. The scheduler identifies an end-of-migration scenario and finalizes the cut-over and termination of the VReplication stream. It is possible for a VReplication migration to span multiple tablets, detailed below. In this case, if the tablet goes down, then the migration will not be lost. It will be continued on another tablet, as described below.
  - `gh-ost`: the executor runs `gh-ost` via `os.Exec`. It runs the entire flow within a single function. Thus, `gh-ost` completes within the same lifetime of the scheduler (and the tablet space in which is operates). To clarify, if the tablet goes down, then the migration is deemed lost.
  - `pt-osc`: the executor runs `pt-online-schema-change` via `os.Exec`. It runs the entire flow within a single function. Thus, `pt-online-schema-change` completes within the same lifetime of the scheduler (and the tablet space in which is operates). To clarify, if the tablet goes down, then the migration is deemed lost.

## Stale migrations

The scheduler maintains a _liveness_ timestamp for running migrations:

- `vitess`/`online` migrations are based on VReplication, which reports last timestamp/transaction timestamp. The scheduler infers migration liveness based on these and on the stream status.
- `gh-ost` migrations report liveness via `/schema-migration/report-status`
- `pt-osc` does not report liveness. The scheduler actively checks for liveness by looking up the `pt-online-schema-change` process.

One way or another, we expect at most (roughly) a 1 minute interval between a running migration's liveness reports. When a migration is expected to be running, and does not have a liveness report for `10` minutes, then it is considered _stale_.

A stale migration can happen for various reasons. Perhaps a `pt-osc` process went zombie. Or a `gh-ost` process was locked.

When the scheduler finds a stale migration, it:

- Considers it to be broken and removes it from internal bookkeeping of running migrations.
- Takes steps to forcefully terminate it, just in case it still happens to run:
  - For a `gh-ost` migration, it touches the panic flag file.
  - For `pt-osc`, it `kill`s the process, if any
  - For `online`, it stops and deletes the stream

## Failed tablet migrations

A specially handled scenario is where a migration runs, and the owning (primary) tablet fails.

For `gh-ost` and `pt-osc` migrations, it's impossible to resume the migration from the exact point of failure. The scheduler will attempt a full retry of the migration. This means throwing away the previous migration's artifacts (ghost tables) and starting anew.

To avoid a cascading failure scenario, a migration is only auto-retried _once_. If a 2nd tablet failure takes place, it's up to the user to retry the failed migration.

## Cross tablet VReplication migrations

VReplication is more capable than `gh-ost` and `pt-osc`, since it tracks its state transactionally in the same database server as the migration/ghost table. This means a stream can automatically recover after e.g. a failover. The new `primary` tablet has all the information in `_vt.vreplication`, `_vt.copy_state` to keep on running the stream.

The scheduler supports that. It is able to identify a stream which started with a previous tablet, and is able to take ownership of such a stream. Because VReplication will recover/resume a stream independently of the scheduler, the scheduler will then implicitly find that the stream is _running_ and be able to assert its _liveness_.

The result is that if a tablet fails mid-`online` migration, the new `primary` tablet will auto-resume migration _from the point of interruption_. This happens whether it's the same table that recovers as `primary` or whether its a new tablet that is promoted as `primary`. A migration can survive multiple tablet failures. It is only limited by VReplication's capabilities.
