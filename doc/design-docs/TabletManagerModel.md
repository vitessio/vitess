# The tabletmanager model

## Background

The current tabletmanager model treats the tablet record as authoritative. The tabletmanager polls the tablet record and reacts to changes there. There are also calls sprayed around the code that invoke “RefreshTablet” or “RefreshState”, after they update the tablet record.

This model is not representative of how we currently operate vitess. There is actually no benefit to updating the tablet record and expecting the tablet to refresh itself. In fact, the approach is fragile because we’re unnecessarily bringing additional components in our chain of action, thereby increasing the chances of failure.

We should instead change our model to say that the tablet process is the authoritative source of its current state. It publishes it to the tablet record, which is then used for discovery.

## Details

### While vttablet is up

Every flow that needs to change something about a tablet directly issues an rpc request to it. The tablet will immediately execute the request, and will perform a best-effort action to update the tablet record, and will continue to retry until it succeeds.

The main advantage of this approach is that the request will succeed with a single round trip to the tablet. If the request fails, we treat it as a failure. If the request succeeds, but the tablet fails to update its record, we still succeed. The tablet record will eventually be updated.

### If vttablet is down or is unreachable

If vttablet is unreachable, we fail the operation. This failure mode is no worse than us not being able to update a tablet record.

Essentially, we generally assume that the tablet record may not be in sync with the vttablet. This has always been the case, and our code has to deal with this already.

### Refresh State

Refresh state will continue to exist as an API, but it’s only for refreshing against state changes in the global topo.

### Exception 1: Cluster Leadership

In the case of flows that designate who the primary is, the topo is the authority. For such requests, the tablet will first try to update its record, and only then succeed. This is required because of how the new cluster leadership redesign works.

### Exception 2: VTTablet startup

When a vttablet starts up, it will treat the following info from the topo as authoritative:

* Keyspace
* Shard
* Tablet Type
* DBName

As an additional precaution, if this info does not match the init parameters, we should exit.

We can also consider the following: If the tablet type is primary, we can force a sync against the shard record before we confirm ourselves to be the primary.

## Advantages

The main advantage of this approach is that a vttablet becomes the authoritative owner of a tablet record. This will greatly reduce its complexity because it doesn’t have to continuously poll it, and it does not have to deal with possibly unexpected or invalid changes in the tablet record.

Since it can assume that nobody else is modifying the record, the vttablet can freely update the tablet record with its local copy without worrying about synchronizing with the existing info.

This will also simplify many flows because they will all become a single request instead of being two requests (change record and refresh).

Load on topos will be reduced because the tablets don’t poll anymore.

## Transition

We’ll first change all call sites to a single round-trip to the tablets. The tabletmanager will continue to update the tablet record and rely on its existing poller.

This change will mean that the tabletmanager has become the sole owner of its tablet record. At this point, we can switch its behavior to non-polling.
