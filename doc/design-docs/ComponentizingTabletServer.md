# Componentizing TabletServer

As Vitess adoption expands, several feature requests have been popping up that will benefit from multiple instances of TabletServer (or its sub-components) co-existing within the same process.

The following features drive this refactor, in order of priority:

* Multi-schema
* VTShovel: multiple data import sources for vreplication
* VTDirect: allow VTGate to directly send queries to mysql

Beyond these features, componentizing TabletServer will make the vitess architecture more flexible. There are many places in the code where we instantiate a QueryService. All those places can now explore the benefit of instantiating a TabletServer or its sub-components.

## Features

This section describes the use cases and their features. An important prerequisite: In order to retain backward compatibility, the new features should not cause existing behavior to be affected.

### Multi-schema

There has been a steady inflow of enquiries about use cases where a mysql instance has a large number of schemas (1000+). We currently support multi-schema by requiring the user to launch one vttablet process per schema. This, however, does not scale for the number of schemas we are beginning to see.

To enable this, we need the ability for a single vttablet to host multiple TabletServers. Requirements are:

* Grouped or consolidated Stats (/debug/vars).
* Segregated or consolidated HTTP endpoints, like (/debug/status), with sub-page links working.
* A better way to specify flags: the existing approach of command line flags may not scale.
* A tablet manager that can represent multiple tablet ids.

Other parts of TabletServer have already been modified to point at a shared mysql instance due to work done by @deepthi in #4727, and other related changes.

### VTShovel

VTShovel is a data migration feature that extends VReplication to allow a user to specify an external mysql as the source. This can be used to import data and also keep the targets up-to-date as the source is written to.

VTShovel currently has two limitations:

* It supports only one external source per vttablet. The need for multiple sources was voiced as a requirement by one of the adopters.
* It does not support VDiff, which also requires multiple sources.

If the TabletServer refactor is architected correctly, VTShovel should inherit the multi-instance ability without any major impact. In particular:

* Leverage the flags refactor to support more than one external source.
* Observability features implemented for multi-schema like stats and HTTP endpoints should naturally extend to vtshovel.
* VDiff should work.

### VTDirect

The excessive use of CPU due to gRPC continues to be a concern among some adopters. Additionally, Vitess is now being deployed against externally managed databases like RDS and CloudSQL. Such users are reluctant to pay the latency cost of the extra hop.

VTDirect is the ability of VTGate to directly send queries to the mysql instances.

This feature adds the following requirements over the previous ones:

* Some features of TabletServer (like sequences) should be disabled or redirected to an actual vttablet.
* TabletServers can have a life-cycle as tablets are added and removed from the topo. The variables and end-points need to reflect these changes.

In previous discussions, alternate approaches that did not require a TabletServer refactor were suggested. Given that the TabletServer refactor brings us much closer to this feature, we’ll need to re-evaluate our options for the best approach. This will be a separate RFC.

## Requirements

This section describes the requirements dictated by the features.

### Stats

Stats (/debug/vars) should be reported in such a way that the variables from each TabletServer can be differentiated. Idiomatic usage of vitess expects the monitoring tool to add the tablet id as a dimension when combining variables coming from different vttablets. Therefore, every TabletServer should be changed to add this dimension to its exported variables.

On the flip side, this may result in an extremely large number of variables to be exported. If so, it may be better to consolidate them. There is no right answer; We have to support both options.

#### Other options considered

We could have each TabletServer export a brand new set of variables by appending the tablet id to the root variable name. However, this would make it very hard for monitoring tools because they are not very good at dealing with dynamic variable names.

### HTTP endpoints

A TabletServer exports a variety of http endpoints. In general, it makes sense to have each TabletServer export a separate set of endpoints within the current process. However, in cases where the performance of the underlying mysql is concerned, it may be beneficial to consolidate certain pages.

We’ll start with a separate set of pages, with each set prefixed by the tablet id. For example, what was previously `/debug/consolidations` will now become `/cell-100/debug/consolidations`.

#### Other options considered

We could keep the existing set of endpoints unchanged, and have each TabletServer add its section. But this would make it hard to troubleshoot problems related to a specific TabletServer.

The best-case scenario would be the “why not both” option: the original set of pages continue to exist and provide a summary from all the tablet servers. This can still be implemented as an enhancement.

### Command line flags

Extending the command line flags to be able to specify parameters for a thousand tablet servers is not going to be practical.

Using config files is a better option. To prevent verbosity, we can use a hierarchy where the root config specifies initial values for the flags, and the TabletServer specific configs can inherit and override the original ones.

The input file format could be yaml. Also, this is a good opportunity for us to come up with better names.

Since the config option is more powerful and flexible, specifying that file in the command line will supersede all legacy flags.

#### Other options considered

These configs could be hosted in the topo. This is actually viable. There are two reasons why this option takes a backseat:

* We currently don’t have good tooling for managing data in the topo. VTCtld is currently the only way, and people have found it inadequate sometimes.
* There are mechanisms to secure config files, which will allow it to contain secrets like the mysql passwords. This will not be possible in the case of topos.

## Design

We propose to address the above requirements with the following design elements.

### Dimension Dropper

The dimension dropper will be a new feature of the stats package. Its purpose is to remove specific dimensions from any multi-dimensional variable.

We’ll introduce a new command-line flag that takes a list of labels as input, like `-drop_dimensions='Keyspace,ShardName'`. The stats package will then remove that dimension from any variable that refers to it.

In the case of the TabletServer, specifying `TabletID` in the list of dropped dimensions will have the effect of all TabletServers incrementing a common counter instead of different ones under their own tablet id.

The reason for this approach is that there are already other use cases where the number of exported variables is excessive. This allows us to address those use cases also.

It’s possible that this feature is too broad. For example, one may not want to drop the `Keyspace` dimension from all variables. If we encounter such use cases, it should be relatively easy to extend this feature to accommodate more specific rules.

### Exporter

The exporter will be a new feature that will layer between TabletServer and the singleton APIs: stats and http. It will allow you to create exporters that are either anonymous or named.

An anonymous exporter will behave as if you invoked the stats and http directly. Open issue: we’ll need to see if we want to protect from panics due to duplicate registrations.

A named exporter will perform consolidations or segregations depending on the situation:

* In the case of a stats variable, it will create a common underlying variable, and will update the dimension that matches the name of the exporter.
* In the case of http, it will export the end point under a new URL rooted from the name of the exporter.

Currently, the connection pools have a different name based mechanism to export different stats. The exporter functionality should support this prefixing, which will eliminate the boiler-plate code in those components.

A prototype of this implementation (aka embedder) is present in the vtdirect branch. This needs to be cleaned up and ported to the latest code.

There is no need for the exporter to provide the option to consolidate without the name because the dimension dropper can cover that functionality.

It’s possible to achieve backward compatibility for stats by creating an exporter with a name (tablet id), and then dropping that dimension in stats. However, it’ll work only for stats and not for the http endpoints. For this reason, we need to support explicit code paths for the anonymous exporters. Plus, it makes things more explicit.

### Config loader

The TabletServer already has most, if not all, of its input flags consolidated into a `Config` struct under tabletenv. The existing flags initialize a `DefaultConfig` global variable. If the command line specifies a newly defined flag, like `-tablet_config='filename.yaml'`, then we can branch off into code that reads the yaml file and initializes the configs from there.

The code will load the global part of the yaml into a “global” Config. For each tablet specific config, the global config will be copied first, and then the tablet specific overrides will be overwritten into the copied values.

This is an opportunity for us to rename the members of the Config struct to use better names and data types. The yaml tags will have to match these new names.

The most popular yaml reader seems to be https://github.com/go-yaml/yaml. We’ll start with that and iterate forward.

The dbconfigs data structure will also be folded into the `Config`. This is because each tablet could potentially have different credentials.

#### Bonus points

Given that vitess uses protos everywhere, we could look at standardizing on a generic way to convert yaml to and from protos. This will allow us to look at converting all formats to yaml. If this sounds viable, we can convert the `Config` struct to be generated from a proto, and then have yaml tags that can convert into it. This will future-proof us in case we decide to go this route.

On the initial search, there is no standard way to do this conversion. It would be nice if protos supported this natively as they do for json. We do have the option of using this code to build our own yaml to proto converter: https://github.com/golang/protobuf/blob/master/jsonpb/encode.go.

### TabletManager

The TabletManager change will put everything together for the multi-schema feature.

The ActionAgent data structure will be changed to support multiple tablet servers:

* QueryServiceControl will become a list (or map)
* UpdateStream will be deleted (deprecated)
* TabletAlias, VREngine, _tablet and _blacklistedTables will be added to the QueryServiceControl list

All other members of TabletManager seem unaffected.

The tablet manager API will be extended for cases where requests are specific to a tablet id. For example, `GetSchema` will now require the tablet id as an additional parameter. For legacy support: if tablet id is empty, then we redirect the request to the only tablet.

Note: VREngine’s queries are actually tablet agnostic. The user is expected to restrict their queries to the dbname of the tablet. This is not a good user experience. We should tighten up the query analyzer of vrengine to add dbname as an additional constraint or fill in the correct value as needed.

### VReplication

VReplication should have a relatively easy change. We already have a field named external mysql. This can be a key into the tablet id Config, which can then be used to pull the mysql credentials needed to connect to the external mysql.

The multi-instance capabilities of VStreamer will naturally extend to support all the observability features we’ll add to it.
