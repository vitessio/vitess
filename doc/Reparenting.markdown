Reparenting

This document describes the reparenting features of Vitess. Reparenting is used when the master for a Shard is
changing from one host to another. It can be triggered (for maintenance for instance) or happen automatically
(based on the current master dying for instance).

Two main types of reparenting supported by Vitess are Active Reparents (the Vitess toolchain is handing it all)
and External Reparents (another tool is responsible for reparenting, and the Vitess toolchain just update its
internal state.

Active Reparents

They are triggered by using the 'vtctl ReparentShard' command. See the help for that command.

External Reparents

In this part, we assume another tool has been reparenting our servers. We then trigger the
'vtctl ShardExternallyReparented' command.
