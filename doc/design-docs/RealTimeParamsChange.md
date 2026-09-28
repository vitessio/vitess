# Real-time parameter change

Vitess Components currently disallow changing of configuration parameters while a process is running. There are a few reasons why this was not allowed:

* The command line used to launch the process will cease to be an authoritative source.
* It is difficult to audit any values that were changed in real-time or keep track of history if changes were made multiple times.
* If a process is restarted for any reason, the changes will be reverted.

However, if there is an ongoing high severity incident, it may be beneficial to allow human operators to make temporary changes to a system. This, of course, comes with the caveat that they will eventually revert the changes or make them permanent by deploying the binaries with the new flags.

## Proposal

Given that this should be generally discouraged as common practice, these capabilities should not be made openly available. For example, you should not be able to make such changes using SQL statements.

We’ll export a `/debug/env` endpoint from vttablet, protected by `ADMIN` access. The URL will display the current values of the various parameters and will allow you to modify and submit any of those.

Values like connection pool sizes, query timeouts, query consolidation settings, etc. will be changeable. We’ll iterate on this list as more use cases arise. The initial list will be in an upcoming PR.

## Other options considered

SET Statements: These would be issued via VTGate. The problem with this approach is that the commands would be too widely available. Also, VTGate doesn’t allow targeting of specific tablets.

vtctld ExecuteFetchAsDba SET statements: This could be made to work. However, this is currently implemented as a pass-through in tablet manager. Significant changes will be needed to parse the statements and make them do other things.

vtcltd alternate command, like `SetTabletEnv`: This could be made to work. However, it is a lot more work than what is proposed, and may not be worth it for a feature that is meant to be used so rarely. We still have the option of implementing this if the need arises in the future.
