# VTGate V3 features

## Overview
Historically, Vitess was built from underneath YouTube. This required us to take an iterative approach, which resulted in many versions:
* V0: This version had no VTGate. The application was expected to talk directly to the tablet servers. So, it had to know the sharding scheme, topology, etc.
* V1: This was the first version of VTGate. In this version, the app only needed to know the number of shards, and how to map the sharding key to the correct shard. The rest was done by VTGate. In this version, the app was still exposed to resharding events.
* V2: In this version, the keyspace id was required instead of the shard. This allowed the app to be agnostic of the number of shards.

In the V3 version, the app does not need to specify any routing info. It just sends the query to VTGate as if it's a single database.
