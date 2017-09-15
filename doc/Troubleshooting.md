If there is a problem in the system, one or many alerts would typically fire. If a problem was found through means other than an alert, then the alert system needs to be iterated upon.

When an alert fires, you have the following sources of information to perform your investigation:

* Alert values
* Graphs
* Diagnostic URLs
* Log files

Below are a few possible scenarios.

### Elevated query latency on master

Diagnosis 1: Inspect the graphs to see if QPS has gone up. If yes, drill down on the more detailed QPS graphs to see which table, or user caused the increase. If a table is identified, look at /debug/queryz for queries on that table.

Action: Inform engineer about about toxic query. If it’s a specific user, you can stop their job or throttle them to keep the load manageable. As a last resort, blacklist query to allow the rest of the system to stay healthy.

Diagnosis 2: QPS did not go up, only latency did. Inspect the per-table latency graphs. If it’s a specific table, then it’s most likely a long-running low QPS query that’s skewing the numbers. Identify the culprit query and take necessary steps to get it optimized. Such queries usually do not cause outage. So, there may not be a need to take extreme measures.

Diagnosis 3: Latency seems to be up across the board. Inspect transaction latency. If this has gone up, then something is causing MySQL to run too many concurrent transactions which causes slow-down. See if there are any tx pool full errors. If there is an increase, the INFO logs will dump info about all transactions. From there, you should be able to if a specific sequence of statements is causing the problem. Once that is identified, find out the root cause. It could be network issues, or it could be a recent change in app behavior.

Diagnosis 4: No particular transaction seems to be the culprit. Nothing seems to have changed in any of the requests. Look at system variables to see if there are hardware faults. Is the disk latency too high? Are there memory parity errors? If so, you may have to failover to a new machine.

### Master starts up read-only

To prevent accidentally accepting writes, our default `my.cnf` settings
tell MySQL to always start up read-only. If the master MySQL gets restarted,
it will thus come back read-only until you intervene to confirm that it should
accept writes. You can use the [SetReadWrite]({% link reference/vtctl.md %}#setreadwrite)
command to do that.

However, usually if something unexpected happens to the master, it's better to
reparent to a different replica with [EmergencyReparentShard]({% link reference/vtctl.md %}#emergencyreparentshard). If you need to do planned maintenance on the master,
it's best to first reparent to another replica with [PlannedReparentShard]({% link reference/vtctl.md %}#plannedreparentshard).

### Vitess sees the wrong tablet as master

If you do a failover manually (not through Vitess), you'll need to tell
Vitess which tablet corresponds to the new master MySQL. Until then,
writes will fail since they'll be routed to a read-only replica
(the old master). Use the [TabletExternallyReparented]({% link reference/vtctl.md %}#tabletexternallyreparented)
command to tell Vitess the new master tablet for a shard.

Tools like [Orchestrator](https://github.com/github/orchestrator)
can be configured to call this automatically when a failover occurs.
See our sample [orchestrator.conf.json](https://github.com/youtube/vitess/blob/1129d69282bb738c94b8af661b984b6377a759f7/docker/orchestrator/orchestrator.conf.json#L131)
for an example of this.
