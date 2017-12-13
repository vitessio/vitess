This reference guide explains the commands that the <b>vtctl</b> tool supports. **vtctl** is a command-line tool used to administer a Vitess cluster, and it allows a human or application to easily interact with a Vitess implementation.

Commands are listed in the following groups:

* [Cells](#cells)
* [Generic](#generic)
* [Keyspaces](#keyspaces)
* [Queries](#queries)
* [Replication Graph](#replication-graph)
* [Resharding Throttler](#resharding-throttler)
* [Schema, Version, Permissions](#schema-version-permissions)
* [Serving Graph](#serving-graph)
* [Shards](#shards)
* [Tablets](#tablets)
* [Topo](#topo)
* [Workflows](#workflows)


## Cells

* [AddCellInfo](#addcellinfo)
* [DeleteCellInfo](#deletecellinfo)
* [GetCellInfo](#getcellinfo)
* [GetCellInfoNames](#getcellinfonames)
* [UpdateCellInfo](#updatecellinfo)

### AddCellInfo

Registers a local topology service in a new cell by creating the CellInfo with the provided parameters. The address will be used to connect to the topology service, and we'll put Vitess data starting at the provided root.

#### Example

<pre class="command-example">AddCellInfo [-server_address &lt;addr&gt;] [-root &lt;root&gt;] &lt;cell&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| root | string | The root path the topology server is using for that cell. |
| server_address | string | The address the topology server is using for that cell. |


#### Arguments

* <code>&lt;addr&gt;</code> &ndash; Required.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;AddCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.


### DeleteCellInfo

Deletes the CellInfo for the provided cell. The cell cannot be referenced by any Shard record.

#### Example

<pre class="command-example">DeleteCellInfo &lt;cell&gt;</pre>

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;DeleteCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetCellInfo

Prints a JSON representation of the CellInfo for a cell.

#### Example

<pre class="command-example">GetCellInfo &lt;cell&gt;</pre>

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;GetCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetCellInfoNames

Lists all the cells for which we have a CellInfo object, meaning we have a local topology service registered.

#### Example

<pre class="command-example">GetCellInfoNames </pre>

#### Errors

* <code>&lt;GetCellInfoNames&gt;</code> command takes no parameter This error occurs if the command is not called with exactly 0 arguments.


### UpdateCellInfo

Updates the content of a CellInfo with the provided parameters. If a value is empty, it is not updated. The CellInfo will be created if it doesn't exist.

#### Example

<pre class="command-example">UpdateCellInfo [-server_address &lt;addr&gt;] [-root &lt;root&gt;] &lt;cell&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| root | string | The root path the topology server is using for that cell. |
| server_address | string | The address the topology server is using for that cell. |


#### Arguments

* <code>&lt;addr&gt;</code> &ndash; Required.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;UpdateCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.


## Generic

* [ListAllTablets](#listalltablets)
* [ListTablets](#listtablets)
* [Validate](#validate)

### ListAllTablets

Lists all tablets in an awk-friendly way.

#### Example

<pre class="command-example">ListAllTablets &lt;cell name&gt;</pre>

#### Arguments

* <code>&lt;cell name&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell name&gt;</code> argument is required for the <code>&lt;ListAllTablets&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ListTablets

Lists specified tablets in an awk-friendly way.

#### Example

<pre class="command-example">ListTablets &lt;tablet alias&gt; ...</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;ListTablets&gt;</code> command This error occurs if the command is not called with at least one argument.


### Validate

Validates that all nodes reachable from the global replication graph and that all tablets in all discoverable cells are consistent.

#### Example

<pre class="command-example">Validate [-ping-tablets]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| ping-tablets | Boolean | Indicates whether all tablets should be pinged during the validation process |




## Keyspaces

* [CreateKeyspace](#createkeyspace)
* [DeleteKeyspace](#deletekeyspace)
* [FindAllShardsInKeyspace](#findallshardsinkeyspace)
* [GetKeyspace](#getkeyspace)
* [GetKeyspaces](#getkeyspaces)
* [MigrateServedFrom](#migrateservedfrom)
* [MigrateServedTypes](#migrateservedtypes)
* [RebuildKeyspaceGraph](#rebuildkeyspacegraph)
* [RemoveKeyspaceCell](#removekeyspacecell)
* [SetKeyspaceServedFrom](#setkeyspaceservedfrom)
* [SetKeyspaceShardingInfo](#setkeyspaceshardinginfo)
* [ValidateKeyspace](#validatekeyspace)
* [WaitForDrain](#waitfordrain)

### CreateKeyspace

Creates the specified keyspace.

#### Example

<pre class="command-example">CreateKeyspace [-sharding_column_name=name] [-sharding_column_type=type] [-served_from=tablettype1:ks1,tablettype2,ks2,...] [-force] &lt;keyspace name&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds even if the keyspace already exists |
| served_from | string | Specifies a comma-separated list of dbtype:keyspace pairs used to serve traffic |
| sharding_column_name | string | Specifies the column to use for sharding operations |
| sharding_column_type | string | Specifies the type of the column to use for sharding operations |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;CreateKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### DeleteKeyspace

Deletes the specified keyspace. In recursive mode, it also recursively deletes all shards in the keyspace. Otherwise, there must be no shards left in the keyspace.

#### Example

<pre class="command-example">DeleteKeyspace [-recursive] &lt;keyspace&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| recursive | Boolean | Also recursively delete all shards in the keyspace. |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* must specify the <code>&lt;keyspace&gt;</code> argument for <code>&lt;DeleteKeyspace&gt;</code> This error occurs if the command is not called with exactly one argument.


### FindAllShardsInKeyspace

Displays all of the shards in the specified keyspace.

#### Example

<pre class="command-example">FindAllShardsInKeyspace &lt;keyspace&gt;</pre>

#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;FindAllShardsInKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetKeyspace

Outputs a JSON structure that contains information about the Keyspace.

#### Example

<pre class="command-example">GetKeyspace &lt;keyspace&gt;</pre>

#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;GetKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetKeyspaces

Outputs a sorted list of all keyspaces.



### MigrateServedFrom

Makes the &lt;destination keyspace/shard&gt; serve the given type. This command also rebuilds the serving graph.

#### Example

<pre class="command-example">MigrateServedFrom [-cells=c1,c2,...] [-reverse] &lt;destination keyspace/shard&gt; &lt;served tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to update |
| filtered_replication_wait_time | Duration | Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations |
| reverse | Boolean | Moves the served tablet type backward instead of forward. Use in case of trouble |


#### Arguments

* <code>&lt;destination keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;served tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;destination keyspace/shard&gt;</code> and <code>&lt;served tablet type&gt;</code> arguments are both required for the <code>&lt;MigrateServedFrom&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### MigrateServedTypes

Migrates a serving type from the source shard to the shards that it replicates to. This command also rebuilds the serving graph. The &lt;keyspace/shard&gt; argument can specify any of the shards involved in the migration.

#### Example

<pre class="command-example">MigrateServedTypes [-cells=c1,c2,...] [-reverse] [-skip-refresh-state] &lt;keyspace/shard&gt; &lt;served tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to update |
| filtered_replication_wait_time | Duration | Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations |
| reverse | Boolean | Moves the served tablet type backward instead of forward. Use in case of trouble |
| skip-refresh-state | Boolean | Skips refreshing the state of the source tablets after the migration, meaning that the refresh will need to be done manually, replica and rdonly only) |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;served tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;source keyspace/shard&gt;</code> and <code>&lt;served tablet type&gt;</code> arguments are both required for the <code>&lt;MigrateServedTypes&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* the <code>&lt;skip-refresh-state&gt;</code> flag can only be specified for non-master migrations


### RebuildKeyspaceGraph

Rebuilds the serving data for the keyspace. This command may trigger an update to all connected clients.

#### Example

<pre class="command-example">RebuildKeyspaceGraph [-cells=c1,c2,...] &lt;keyspace&gt; ...</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to update |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument must be used to specify at least one keyspace when calling the <code>&lt;RebuildKeyspaceGraph&gt;</code> command This error occurs if the command is not called with at least one argument.


### RemoveKeyspaceCell

Removes the cell from the Cells list for all shards in the keyspace, and the SrvKeyspace for that keyspace in that cell.

#### Example

<pre class="command-example">RemoveKeyspaceCell [-force] [-recursive] &lt;keyspace&gt; &lt;cell&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data. |
| recursive | Boolean | Also delete all tablets in that cell belonging to the specified keyspace. |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> and <code>&lt;cell&gt;</code> arguments are required for the <code>&lt;RemoveKeyspaceCell&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### SetKeyspaceServedFrom

Changes the ServedFromMap manually. This command is intended for emergency fixes. This field is automatically set when you call the *MigrateServedFrom* command. This command does not rebuild the serving graph.

#### Example

<pre class="command-example">SetKeyspaceServedFrom [-source=&lt;source keyspace name&gt;] [-remove] [-cells=c1,c2,...] &lt;keyspace name&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to affect |
| remove | Boolean | Indicates whether to add (default) or remove the served from record |
| source | string | Specifies the source keyspace name |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;keyspace name&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are required for the <code>&lt;SetKeyspaceServedFrom&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### SetKeyspaceShardingInfo

Updates the sharding information for a keyspace.

#### Example

<pre class="command-example">SetKeyspaceShardingInfo [-force] &lt;keyspace name&gt; [&lt;column name&gt;] [&lt;column type&gt;]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Updates fields even if they are already set. Use caution before calling this command. |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;column name&gt;</code> &ndash; Optional.
* <code>&lt;column type&gt;</code> &ndash; Optional.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;SetKeyspaceShardingInfo&gt;</code> command. The <code>&lt;column name&gt;</code> and <code>&lt;column type&gt;</code> arguments are both optional This error occurs if the command is not called with between 1 and 3 arguments.
* both <code>&lt;column name&gt;</code> and <code>&lt;column type&gt;</code> must be set, or both must be unset


### ValidateKeyspace

Validates that all nodes reachable from the specified keyspace are consistent.

#### Example

<pre class="command-example">ValidateKeyspace [-ping-tablets] &lt;keyspace name&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| ping-tablets | Boolean | Specifies whether all tablets will be pinged during the validation process |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidateKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### WaitForDrain

Blocks until no new queries were observed on all tablets with the given tablet type in the specifed keyspace.  This can be used as sanity check to ensure that the tablets were drained after running vtctl MigrateServedTypes  and vtgate is no longer using them. If -timeout is set, it fails when the timeout is reached.

#### Example

<pre class="command-example">WaitForDrain [-timeout &lt;duration&gt;] [-retry_delay &lt;duration&gt;] [-initial_wait &lt;duration&gt;] &lt;keyspace/shard&gt; &lt;served tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to look for tablets |
| initial_wait | Duration | Time to wait for all tablets to check in |
| retry_delay | Duration | Time to wait between two checks |
| timeout | Duration | Timeout after which the command fails |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;served tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are both required for the <code>&lt;WaitForDrain&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


## Queries

* [VtGateExecute](#vtgateexecute)
* [VtGateExecuteKeyspaceIds](#vtgateexecutekeyspaceids)
* [VtGateExecuteShards](#vtgateexecuteshards)
* [VtGateSplitQuery](#vtgatesplitquery)
* [VtTabletBegin](#vttabletbegin)
* [VtTabletCommit](#vttabletcommit)
* [VtTabletExecute](#vttabletexecute)
* [VtTabletRollback](#vttabletrollback)
* [VtTabletStreamHealth](#vttabletstreamhealth)
* [VtTabletUpdateStream](#vttabletupdatestream)

### VtGateExecute

Executes the given SQL query with the provided bound variables against the vtgate server.

#### Example

<pre class="command-example">VtGateExecute -server &lt;vtgate&gt; [-bind_variables &lt;JSON map&gt;] [-keyspace &lt;default keyspace&gt;] [-tablet_type &lt;tablet type&gt;] [-options &lt;proto text options&gt;] [-json] &lt;sql&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| json | Boolean | Output JSON instead of human-readable table |
| options | string | execute options values as a text encoded proto of the ExecuteOptions structure |
| server | string | VtGate server to connect to |
| target | string | keyspace:shard@tablet_type |


#### Arguments

* <code>&lt;vtgate&gt;</code> &ndash; Required.
* <code>&lt;sql&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;sql&gt;</code> argument is required for the <code>&lt;VtGateExecute&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* error connecting to vtgate '%v': %v
* Execute failed: %v


### VtGateExecuteKeyspaceIds

Executes the given SQL query with the provided bound variables against the vtgate server. It is routed to the shards that contain the provided keyspace ids.

#### Example

<pre class="command-example">VtGateExecuteKeyspaceIds -server &lt;vtgate&gt; -keyspace &lt;keyspace&gt; -keyspace_ids &lt;ks1 in hex&gt;,&lt;k2 in hex&gt;,... [-bind_variables &lt;JSON map&gt;] [-tablet_type &lt;tablet type&gt;] [-options &lt;proto text options&gt;] [-json] &lt;sql&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| json | Boolean | Output JSON instead of human-readable table |
| keyspace | string | keyspace to send query to |
| keyspace_ids | string | comma-separated list of keyspace ids (in hex) that will map into shards to send query to |
| options | string | execute options values as a text encoded proto of the ExecuteOptions structure |
| server | string | VtGate server to connect to |
| tablet_type | string | tablet type to query |


#### Arguments

* <code>&lt;vtgate&gt;</code> &ndash; Required.
* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;ks1 in hex&gt;</code> &ndash; Required. To specify multiple values for this argument, separate individual values with a comma.
* <code>&lt;sql&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;sql&gt;</code> argument is required for the <code>&lt;VtGateExecuteKeyspaceIds&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot hex-decode value %v '%v': %v
* error connecting to vtgate '%v': %v
* Execute failed: %v


### VtGateExecuteShards

Executes the given SQL query with the provided bound variables against the vtgate server. It is routed to the provided shards.

#### Example

<pre class="command-example">VtGateExecuteShards -server &lt;vtgate&gt; -keyspace &lt;keyspace&gt; -shards &lt;shard0&gt;,&lt;shard1&gt;,... [-bind_variables &lt;JSON map&gt;] [-tablet_type &lt;tablet type&gt;] [-options &lt;proto text options&gt;] [-json] &lt;sql&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| json | Boolean | Output JSON instead of human-readable table |
| keyspace | string | keyspace to send query to |
| options | string | execute options values as a text encoded proto of the ExecuteOptions structure |
| server | string | VtGate server to connect to |
| shards | string | comma-separated list of shards to send query to |
| tablet_type | string | tablet type to query |


#### Arguments

* <code>&lt;vtgate&gt;</code> &ndash; Required.
* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;shard&gt;</code> &ndash; Required. The name of a shard. The argument value is typically in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>. To specify multiple values for this argument, separate individual values with a comma.
* <code>&lt;sql&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;sql&gt;</code> argument is required for the <code>&lt;VtGateExecuteShards&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* error connecting to vtgate '%v': %v
* Execute failed: %v


### VtGateSplitQuery

Executes the SplitQuery computation for the given SQL query with the provided bound variables against the vtgate server (this is the base query for Map-Reduce workloads, and is provided here for debug / test purposes).

#### Example

<pre class="command-example">VtGateSplitQuery -server &lt;vtgate&gt; -keyspace &lt;keyspace&gt; [-split_column &lt;split_column&gt;] -split_count &lt;split_count&gt; [-bind_variables &lt;JSON map&gt;] &lt;sql&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| algorithm | string | The algorithm to |
| keyspace | string | keyspace to send query to |
| server | string | VtGate server to connect to |
| split_count | Int64 | number of splits to generate. |


#### Arguments

* <code>&lt;vtgate&gt;</code> &ndash; Required.
* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;split_count&gt;</code> &ndash; Required.
* <code>&lt;sql&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;sql&gt;</code> argument is required for the <code>&lt;VtGateSplitQuery&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* Exactly one of <code>&lt;split_count&gt;</code> or num_rows_per_query_part
* Unknown split-query <code>&lt;algorithm&gt;</code>: %v
* error connecting to vtgate '%v': %v
* Execute failed: %v
* SplitQuery failed: %v


### VtTabletBegin

Starts a transaction on the provided server.

#### Example

<pre class="command-example">VtTabletBegin [-username &lt;TableACL user&gt;] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> argument is required for the <code>&lt;VtTabletBegin&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v
* Begin failed: %v


### VtTabletCommit

Commits the given transaction on the provided server.

#### Example

<pre class="command-example">VtTabletCommit [-username &lt;TableACL user&gt;] &lt;transaction_id&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;transaction_id&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> and <code>&lt;transaction_id&gt;</code> arguments are required for the <code>&lt;VtTabletCommit&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v


### VtTabletExecute

Executes the given query on the given tablet. -transaction_id is optional. Use VtTabletBegin to start a transaction.

#### Example

<pre class="command-example">VtTabletExecute [-username &lt;TableACL user&gt;] [-transaction_id &lt;transaction_id&gt;] [-options &lt;proto text options&gt;] [-json] &lt;tablet alias&gt; &lt;sql&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| json | Boolean | Output JSON instead of human-readable table |
| options | string | execute options values as a text encoded proto of the ExecuteOptions structure |
| transaction_id | Int | transaction id to use, if inside a transaction. |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;sql&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> and <code>&lt;sql&gt;</code> arguments are required for the <code>&lt;VtTabletExecute&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v
* Execute failed: %v


### VtTabletRollback

Rollbacks the given transaction on the provided server.

#### Example

<pre class="command-example">VtTabletRollback [-username &lt;TableACL user&gt;] &lt;tablet alias&gt; &lt;transaction_id&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;transaction_id&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> and <code>&lt;transaction_id&gt;</code> arguments are required for the <code>&lt;VtTabletRollback&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v


### VtTabletStreamHealth

Executes the StreamHealth streaming query to a vttablet process. Will stop after getting &lt;count&gt; answers.

#### Example

<pre class="command-example">VtTabletStreamHealth [-count &lt;count, default 1&gt;] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| count | Int | number of responses to wait for |


#### Arguments

* <code>&lt;count default 1&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;VtTabletStreamHealth&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v


### VtTabletUpdateStream

Executes the UpdateStream streaming query to a vttablet process. Will stop after getting &lt;count&gt; answers.

#### Example

<pre class="command-example">VtTabletUpdateStream [-count &lt;count, default 1&gt;] [-position &lt;position&gt;] [-timestamp &lt;timestamp&gt;] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| count | Int | number of responses to wait for |
| position | string | position to start the stream from |
| timestamp | Int | timestamp to start the stream from |


#### Arguments

* <code>&lt;count default 1&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;VtTabletUpdateStream&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v


## Replication Graph

* [GetShardReplication](#getshardreplication)

### GetShardReplication

Outputs a JSON structure that contains information about the ShardReplication.

#### Example

<pre class="command-example">GetShardReplication &lt;cell&gt; &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;cell&gt;</code> and <code>&lt;keyspace/shard&gt;</code> arguments are required for the <code>&lt;GetShardReplication&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


## Resharding Throttler

* [GetThrottlerConfiguration](#getthrottlerconfiguration)
* [ResetThrottlerConfiguration](#resetthrottlerconfiguration)
* [ThrottlerMaxRates](#throttlermaxrates)
* [ThrottlerSetMaxRate](#throttlersetmaxrate)
* [UpdateThrottlerConfiguration](#updatethrottlerconfiguration)

### GetThrottlerConfiguration

Returns the current configuration of the MaxReplicationLag module. If no throttler name is specified, the configuration of all throttlers will be returned.

#### Example

<pre class="command-example">GetThrottlerConfiguration -server &lt;vtworker or vttablet&gt; [&lt;throttler name&gt;]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;throttler name&gt;</code> &ndash; Optional.

#### Errors

* the <code>&lt;GetThrottlerConfiguration&gt;</code> command accepts only <code>&lt;throttler name&gt;</code> as optional positional parameter This error occurs if the command is not called with more than 1 arguments.
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to get the throttler configuration from <code>&lt;server&gt;</code> '%v': %v


### ResetThrottlerConfiguration

Resets the current configuration of the MaxReplicationLag module. If no throttler name is specified, the configuration of all throttlers will be reset.

#### Example

<pre class="command-example">ResetThrottlerConfiguration -server &lt;vtworker or vttablet&gt; [&lt;throttler name&gt;]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;throttler name&gt;</code> &ndash; Optional.

#### Errors

* the <code>&lt;ResetThrottlerConfiguration&gt;</code> command accepts only <code>&lt;throttler name&gt;</code> as optional positional parameter This error occurs if the command is not called with more than 1 arguments.
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to get the throttler configuration from <code>&lt;server&gt;</code> '%v': %v


### ThrottlerMaxRates

Returns the current max rate of all active resharding throttlers on the server.

#### Example

<pre class="command-example">ThrottlerMaxRates -server &lt;vtworker or vttablet&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.

#### Errors

* the ThrottlerSetMaxRate command does not accept any positional parameters This error occurs if the command is not called with exactly 0 arguments.
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to get the throttler rate from <code>&lt;server&gt;</code> '%v': %v


### ThrottlerSetMaxRate

Sets the max rate for all active resharding throttlers on the server.

#### Example

<pre class="command-example">ThrottlerSetMaxRate -server &lt;vtworker or vttablet&gt; &lt;rate&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;rate&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;rate&gt;</code> argument is required for the <code>&lt;ThrottlerSetMaxRate&gt;</code> command This error occurs if the command is not called with exactly one argument.
* failed to parse rate '%v' as integer value: %v
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to set the throttler rate on <code>&lt;server&gt;</code> '%v': %v


### UpdateThrottlerConfiguration

Updates the configuration of the MaxReplicationLag module. The configuration must be specified as protobuf text. If a field is omitted or has a zero value, it will be ignored unless -copy_zero_values is specified. If no throttler name is specified, all throttlers will be updated.

#### Example

<pre class="command-example">UpdateThrottlerConfiguration `-server &lt;vtworker or vttablet&gt; [-copy_zero_values] "&lt;configuration protobuf text&gt;" [&lt;throttler name&gt;]`</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| copy_zero_values | Boolean | If true, fields with zero values will be copied as well |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;throttler name&gt;</code> &ndash; Optional.

#### Errors

* Failed to unmarshal the configuration protobuf text (%v) into a protobuf instance: %v
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to update the throttler configuration on <code>&lt;server&gt;</code> '%v': %v


## Schema, Version, Permissions

* [ApplySchema](#applyschema)
* [ApplyVSchema](#applyvschema)
* [CopySchemaShard](#copyschemashard)
* [GetPermissions](#getpermissions)
* [GetSchema](#getschema)
* [GetVSchema](#getvschema)
* [RebuildVSchemaGraph](#rebuildvschemagraph)
* [ReloadSchema](#reloadschema)
* [ReloadSchemaKeyspace](#reloadschemakeyspace)
* [ReloadSchemaShard](#reloadschemashard)
* [ValidatePermissionsKeyspace](#validatepermissionskeyspace)
* [ValidatePermissionsShard](#validatepermissionsshard)
* [ValidateSchemaKeyspace](#validateschemakeyspace)
* [ValidateSchemaShard](#validateschemashard)
* [ValidateVersionKeyspace](#validateversionkeyspace)
* [ValidateVersionShard](#validateversionshard)

### ApplySchema

Applies the schema change to the specified keyspace on every master, running in parallel on all shards. The changes are then propagated to slaves via replication. If -allow_long_unavailability is set, schema changes affecting a large number of rows (and possibly incurring a longer period of unavailability) will not be rejected.

#### Example

<pre class="command-example">ApplySchema [-allow_long_unavailability] [-wait_slave_timeout=10s] {-sql=&lt;sql&gt; || -sql-file=&lt;filename&gt;} &lt;keyspace&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| allow_long_unavailability | Boolean | Allow large schema changes which incur a longer unavailability of the database. |
| sql | string | A list of semicolon-delimited SQL commands |
| sql-file | string | Identifies the file that contains the SQL commands |
| wait_slave_timeout | Duration | The amount of time to wait for slaves to receive the schema change via replication. |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the command<code>&lt;ApplySchema&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ApplyVSchema

Applies the VTGate routing schema to the provided keyspace. Shows the result after application.

#### Example

<pre class="command-example">ApplyVSchema {-vschema=&lt;vschema&gt; || -vschema_file=&lt;vschema file&gt;} [-cells=c1,c2,...] [-skip_rebuild] &lt;keyspace&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | If specified, limits the rebuild to the cells, after upload. Ignored if skipRebuild is set. |
| skip_rebuild | Boolean | If set, do no rebuild the SrvSchema objects. |
| vschema | string | Identifies the VTGate routing schema |
| vschema_file | string | Identifies the VTGate routing schema file |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;ApplyVSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.
* either the <code>&lt;vschema&gt;</code> or <code>&lt;vschema&gt;</code>File flag must be specified when calling the <code>&lt;ApplyVSchema&gt;</code> command


### CopySchemaShard

Copies the schema from a source shard's master (or a specific tablet) to a destination shard. The schema is applied directly on the master of the destination shard, and it is propagated to the replicas through binlogs.

#### Example

<pre class="command-example">CopySchemaShard [-tables=&lt;table1&gt;,&lt;table2&gt;,...] [-exclude_tables=&lt;table1&gt;,&lt;table2&gt;,...] [-include-views] [-wait_slave_timeout=10s] {&lt;source keyspace/shard&gt; || &lt;source tablet alias&gt;} &lt;destination keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the output |
| tables | string | Specifies a comma-separated list of tables to copy. Each is either an exact match, or a regular expression of the form /regexp/ |
| wait_slave_timeout | Duration | The amount of time to wait for slaves to receive the schema change via replication. |


#### Arguments

* <code>&lt;source tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;destination keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;source keyspace/shard&gt;</code> and <code>&lt;destination keyspace/shard&gt;</code> arguments are both required for the <code>&lt;CopySchemaShard&gt;</code> command. Instead of the <code>&lt;source keyspace/shard&gt;</code> argument, you can also specify <code>&lt;tablet alias&gt;</code> which refers to a specific tablet of the shard in the source keyspace This error occurs if the command is not called with exactly 2 arguments.


### GetPermissions

Displays the permissions for a tablet.

#### Example

<pre class="command-example">GetPermissions &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;GetPermissions&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetSchema

Displays the full schema for a tablet, or just the schema for the specified tables in that tablet.

#### Example

<pre class="command-example">GetSchema [-tables=&lt;table1&gt;,&lt;table2&gt;,...] [-exclude_tables=&lt;table1&gt;,&lt;table2&gt;,...] [-include-views] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the output |
| table_names_only | Boolean | Only displays table names that match |
| tables | string | Specifies a comma-separated list of tables for which we should gather information. Each is either an exact match, or a regular expression of the form /regexp/ |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;GetSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetVSchema

Displays the VTGate routing schema.

#### Example

<pre class="command-example">GetVSchema &lt;keyspace&gt;</pre>

#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;GetVSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.


### RebuildVSchemaGraph

Rebuilds the cell-specific SrvVSchema from the global VSchema objects in the provided cells (or all cells if none provided).

#### Example

<pre class="command-example">RebuildVSchemaGraph [-cells=c1,c2,...]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to look for tablets |


#### Errors

* <code>&lt;RebuildVSchemaGraph&gt;</code> doesn't take any arguments This error occurs if the command is not called with exactly 0 arguments.


### ReloadSchema

Reloads the schema on a remote tablet.

#### Example

<pre class="command-example">ReloadSchema &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;ReloadSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ReloadSchemaKeyspace

Reloads the schema on all the tablets in a keyspace.

#### Example

<pre class="command-example">ReloadSchemaKeyspace [-concurrency=10] [-include_master=false] &lt;keyspace&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| concurrency | Int | How many tablets to reload in parallel |
| include_master | Boolean | Include the master tablet(s) |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;ReloadSchemaKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ReloadSchemaShard

Reloads the schema on all the tablets in a shard.

#### Example

<pre class="command-example">ReloadSchemaShard [-concurrency=10] [-include_master=false] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| concurrency | Int | How many tablets to reload in parallel |
| include_master | Boolean | Include the master tablet |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ReloadSchemaShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ValidatePermissionsKeyspace

Validates that the master permissions from shard 0 match those of all of the other tablets in the keyspace.

#### Example

<pre class="command-example">ValidatePermissionsKeyspace &lt;keyspace name&gt;</pre>

#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidatePermissionsKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ValidatePermissionsShard

Validates that the master permissions match all the slaves.

#### Example

<pre class="command-example">ValidatePermissionsShard &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidatePermissionsShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ValidateSchemaKeyspace

Validates that the master schema from shard 0 matches the schema on all of the other tablets in the keyspace.

#### Example

<pre class="command-example">ValidateSchemaKeyspace [-exclude_tables=''] [-include-views] &lt;keyspace name&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the validation |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidateSchemaKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ValidateSchemaShard

Validates that the master schema matches all of the slaves.

#### Example

<pre class="command-example">ValidateSchemaShard [-exclude_tables=''] [-include-views] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the validation |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidateSchemaShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ValidateVersionKeyspace

Validates that the master version from shard 0 matches all of the other tablets in the keyspace.

#### Example

<pre class="command-example">ValidateVersionKeyspace &lt;keyspace name&gt;</pre>

#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidateVersionKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ValidateVersionShard

Validates that the master version matches all of the slaves.

#### Example

<pre class="command-example">ValidateVersionShard &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidateVersionShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


## Serving Graph

* [GetSrvKeyspace](#getsrvkeyspace)
* [GetSrvKeyspaceNames](#getsrvkeyspacenames)
* [GetSrvVSchema](#getsrvvschema)

### GetSrvKeyspace

Outputs a JSON structure that contains information about the SrvKeyspace.

#### Example

<pre class="command-example">GetSrvKeyspace &lt;cell&gt; &lt;keyspace&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> and <code>&lt;keyspace&gt;</code> arguments are required for the <code>&lt;GetSrvKeyspace&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### GetSrvKeyspaceNames

Outputs a list of keyspace names.

#### Example

<pre class="command-example">GetSrvKeyspaceNames &lt;cell&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;GetSrvKeyspaceNames&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetSrvVSchema

Outputs a JSON structure that contains information about the SrvVSchema.

#### Example

<pre class="command-example">GetSrvVSchema &lt;cell&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;GetSrvVSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.


## Shards

* [CreateShard](#createshard)
* [DeleteShard](#deleteshard)
* [EmergencyReparentShard](#emergencyreparentshard)
* [GetShard](#getshard)
* [InitShardMaster](#initshardmaster)
* [ListBackups](#listbackups)
* [ListShardTablets](#listshardtablets)
* [PlannedReparentShard](#plannedreparentshard)
* [RemoveBackup](#removebackup)
* [RemoveShardCell](#removeshardcell)
* [SetShardServedTypes](#setshardservedtypes)
* [SetShardTabletControl](#setshardtabletcontrol)
* [ShardReplicationFix](#shardreplicationfix)
* [ShardReplicationPositions](#shardreplicationpositions)
* [SourceShardAdd](#sourceshardadd)
* [SourceShardDelete](#sourcesharddelete)
* [TabletExternallyReparented](#tabletexternallyreparented)
* [ValidateShard](#validateshard)
* [WaitForFilteredReplication](#waitforfilteredreplication)

### CreateShard

Creates the specified shard.

#### Example

<pre class="command-example">CreateShard [-force] [-parent] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds with the command even if the keyspace already exists |
| parent | Boolean | Creates the parent keyspace if it doesn't already exist |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;CreateShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


### DeleteShard

Deletes the specified shard(s). In recursive mode, it also deletes all tablets belonging to the shard. Otherwise, there must be no tablets left in the shard.

#### Example

<pre class="command-example">DeleteShard [-recursive] [-even_if_serving] &lt;keyspace/shard&gt; ...</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| even_if_serving | Boolean | Remove the shard even if it is serving. Use with caution. |
| recursive | Boolean | Also delete all tablets belonging to the shard. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument must be used to identify at least one keyspace and shard when calling the <code>&lt;DeleteShard&gt;</code> command This error occurs if the command is not called with at least one argument.


### EmergencyReparentShard

Reparents the shard to the new master. Assumes the old master is dead and not responsding.

#### Example

<pre class="command-example">EmergencyReparentShard -keyspace_shard=&lt;keyspace/shard&gt; -new_master=&lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| keyspace_shard | string | keyspace/shard of the shard that needs to be reparented |
| new_master | string | alias of a tablet that should be the new master |
| wait_slave_timeout | Duration | time to wait for slaves to catch up in reparenting |


#### Errors

* action <code>&lt;EmergencyReparentShard&gt;</code> requires -keyspace_shard=<code>&lt;keyspace/shard&gt;</code> -new_master=<code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly 0 arguments.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)
* cannot use legacy syntax and flag -<code>&lt;new_master&gt;</code> for action <code>&lt;EmergencyReparentShard&gt;</code> at the same time


### GetShard

Outputs a JSON structure that contains information about the Shard.

#### Example

<pre class="command-example">GetShard &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;GetShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


### InitShardMaster

Sets the initial master for a shard. Will make all other tablets in the shard slaves of the provided master. WARNING: this could cause data loss on an already replicating shard. PlannedReparentShard or EmergencyReparentShard should be used instead.

#### Example

<pre class="command-example">InitShardMaster [-force] [-wait_slave_timeout=&lt;duration&gt;] &lt;keyspace/shard&gt; &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | will force the reparent even if the provided tablet is not a master or the shard master |
| wait_slave_timeout | Duration | time to wait for slaves to catch up in reparenting |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* action <code>&lt;InitShardMaster&gt;</code> requires <code>&lt;keyspace/shard&gt;</code> <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly 2 arguments.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)


### ListBackups

Lists all the backups for a shard.

#### Example

<pre class="command-example">ListBackups &lt;keyspace/shard&gt;</pre>

#### Errors

* action <code>&lt;ListBackups&gt;</code> requires <code>&lt;keyspace/shard&gt;</code> This error occurs if the command is not called with exactly one argument.


### ListShardTablets

Lists all tablets in the specified shard.

#### Example

<pre class="command-example">ListShardTablets &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ListShardTablets&gt;</code> command This error occurs if the command is not called with exactly one argument.


### PlannedReparentShard

Reparents the shard to the new master, or away from old master. Both old and new master need to be up and running.

#### Example

<pre class="command-example">PlannedReparentShard -keyspace_shard=&lt;keyspace/shard&gt; [-new_master=&lt;tablet alias&gt;] [-avoid_master=&lt;tablet alias&gt;]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| avoid_master | string | alias of a tablet that should not be the master, i.e. reparent to any other tablet if this one is the master |
| keyspace_shard | string | keyspace/shard of the shard that needs to be reparented |
| new_master | string | alias of a tablet that should be the new master |
| wait_slave_timeout | Duration | time to wait for slaves to catch up in reparenting |


#### Errors

* action <code>&lt;PlannedReparentShard&gt;</code> requires -keyspace_shard=<code>&lt;keyspace/shard&gt;</code> [-new_master=<code>&lt;tablet alias&gt;</code>] [-avoid_master=<code>&lt;tablet alias&gt;</code>] This error occurs if the command is not called with exactly 0 arguments.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)
* cannot use legacy syntax and flags -<code>&lt;keyspace_shard&gt;</code> and -<code>&lt;new_master&gt;</code> for action <code>&lt;PlannedReparentShard&gt;</code> at the same time


### RemoveBackup

Removes a backup for the BackupStorage.

#### Example

<pre class="command-example">RemoveBackup &lt;keyspace/shard&gt; &lt;backup name&gt;</pre>

#### Arguments

* <code>&lt;backup name&gt;</code> &ndash; Required.

#### Errors

* action <code>&lt;RemoveBackup&gt;</code> requires <code>&lt;keyspace/shard&gt;</code> <code>&lt;backup name&gt;</code> This error occurs if the command is not called with exactly 2 arguments.


### RemoveShardCell

Removes the cell from the shard's Cells list.

#### Example

<pre class="command-example">RemoveShardCell [-force] [-recursive] &lt;keyspace/shard&gt; &lt;cell&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data. |
| recursive | Boolean | Also delete all tablets in that cell belonging to the specified shard. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;cell&gt;</code> arguments are required for the <code>&lt;RemoveShardCell&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### SetShardServedTypes

Add or remove served type to/from a shard. This is meant as an emergency function. It does not rebuild any serving graph i.e. does not run 'RebuildKeyspaceGraph'.

#### Example

<pre class="command-example">SetShardServedTypes [--cells=c1,c2,...] [--remove] &lt;keyspace/shard&gt; &lt;served tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to update |
| remove | Boolean | Removes the served tablet type |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;served tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;served tablet type&gt;</code> arguments are both required for the <code>&lt;SetShardServedTypes&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### SetShardTabletControl

Sets the TabletControl record for a shard and type. Only use this for an emergency fix or after a finished vertical split. The *MigrateServedFrom* and *MigrateServedType* commands set this field appropriately already. Always specify the blacklisted_tables flag for vertical splits, but never for horizontal splits.<br><br>To set the DisableQueryServiceFlag, keep 'blacklisted_tables' empty, and set 'disable_query_service' to true or false. Useful to fix horizontal splits gone wrong.<br><br>To change the blacklisted tables list, specify the 'blacklisted_tables' parameter with the new list. Useful to fix tables that are being blocked after a vertical split.<br><br>To just remove the ShardTabletControl entirely, use the 'remove' flag, useful after a vertical split is finished to remove serving restrictions.

#### Example

<pre class="command-example">SetShardTabletControl [--cells=c1,c2,...] [--blacklisted_tables=t1,t2,...] [--remove] [--disable_query_service] &lt;keyspace/shard&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| blacklisted_tables | string | Specifies a comma-separated list of tables to blacklist (used for vertical split). Each is either an exact match, or a regular expression of the form '/regexp/'. |
| cells | string | Specifies a comma-separated list of cells to update |
| disable_query_service | Boolean | Disables query service on the provided nodes. This flag requires 'blacklisted_tables' and 'remove' to be unset, otherwise it's ignored. |
| remove | Boolean | Removes cells for vertical splits. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are both required for the <code>&lt;SetShardTabletControl&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### ShardReplicationFix

Walks through a ShardReplication object and fixes the first error that it encounters.

#### Example

<pre class="command-example">ShardReplicationFix &lt;cell&gt; &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;cell&gt;</code> and <code>&lt;keyspace/shard&gt;</code> arguments are required for the ShardReplicationRemove command This error occurs if the command is not called with exactly 2 arguments.


### ShardReplicationPositions

Shows the replication status of each slave machine in the shard graph. In this case, the status refers to the replication lag between the master vttablet and the slave vttablet. In Vitess, data is always written to the master vttablet first and then replicated to all slave vttablets. Output is sorted by tablet type, then replication position. Use ctrl-C to interrupt command and see partial result if needed.

#### Example

<pre class="command-example">ShardReplicationPositions &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ShardReplicationPositions&gt;</code> command This error occurs if the command is not called with exactly one argument.


### SourceShardAdd

Adds the SourceShard record with the provided index. This is meant as an emergency function. It does not call RefreshState for the shard master.

#### Example

<pre class="command-example">SourceShardAdd [--key_range=&lt;keyrange&gt;] [--tables=&lt;table1,table2,...&gt;] &lt;keyspace/shard&gt; &lt;uid&gt; &lt;source keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| key_range | string | Identifies the key range to use for the SourceShard |
| tables | string | Specifies a comma-separated list of tables to replicate (used for vertical split). Each is either an exact match, or a regular expression of the form /regexp/ |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;uid&gt;</code> &ndash; Required.
* <code>&lt;source keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code>, <code>&lt;uid&gt;</code>, and <code>&lt;source keyspace/shard&gt;</code> arguments are all required for the <code>&lt;SourceShardAdd&gt;</code> command This error occurs if the command is not called with exactly 3 arguments.


### SourceShardDelete

Deletes the SourceShard record with the provided index. This is meant as an emergency cleanup function. It does not call RefreshState for the shard master.

#### Example

<pre class="command-example">SourceShardDelete &lt;keyspace/shard&gt; &lt;uid&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;uid&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;uid&gt;</code> arguments are both required for the <code>&lt;SourceShardDelete&gt;</code> command This error occurs if the command is not called with at least 2 arguments.


### TabletExternallyReparented

Changes metadata in the topology server to acknowledge a shard master change performed by an external tool. See the Reparenting guide for more information:https://github.com/youtube/vitess/blob/master/doc/Reparenting.md#external-reparents.

#### Example

<pre class="command-example">TabletExternallyReparented &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;TabletExternallyReparented&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ValidateShard

Validates that all nodes that are reachable from this shard are consistent.

#### Example

<pre class="command-example">ValidateShard [-ping-tablets] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| ping-tablets | Boolean | Indicates whether all tablets should be pinged during the validation process |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidateShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


### WaitForFilteredReplication

Blocks until the specified shard has caught up with the filtered replication of its source shard.

#### Example

<pre class="command-example">WaitForFilteredReplication [-max_delay &lt;max_delay, default 30s&gt;] &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;WaitForFilteredReplication&gt;</code> command This error occurs if the command is not called with exactly one argument.


## Tablets

* [Backup](#backup)
* [ChangeSlaveType](#changeslavetype)
* [DeleteTablet](#deletetablet)
* [ExecuteFetchAsDba](#executefetchasdba)
* [ExecuteHook](#executehook)
* [GetTablet](#gettablet)
* [IgnoreHealthError](#ignorehealtherror)
* [InitTablet](#inittablet)
* [Ping](#ping)
* [RefreshState](#refreshstate)
* [RefreshStateByShard](#refreshstatebyshard)
* [ReparentTablet](#reparenttablet)
* [RestoreFromBackup](#restorefrombackup)
* [RunHealthCheck](#runhealthcheck)
* [SetReadOnly](#setreadonly)
* [SetReadWrite](#setreadwrite)
* [Sleep](#sleep)
* [StartSlave](#startslave)
* [StopSlave](#stopslave)
* [UpdateTabletAddrs](#updatetabletaddrs)

### Backup

Stops mysqld and uses the BackupStorage service to store a new backup. This function also remembers if the tablet was replicating so that it can restore the same state after the backup completes.

#### Example

<pre class="command-example">Backup [-concurrency=4] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| concurrency | Int | Specifies the number of compression/checksum jobs to run simultaneously |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;Backup&gt;</code> command requires the <code>&lt;tablet alias&gt;</code> argument This error occurs if the command is not called with exactly one argument.


### ChangeSlaveType

Changes the db type for the specified tablet, if possible. This command is used primarily to arrange replicas, and it will not convert a master.<br><br>NOTE: This command automatically updates the serving graph.<br><br>

#### Example

<pre class="command-example">ChangeSlaveType [-dry-run] &lt;tablet alias&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| dry-run | Boolean | Lists the proposed change without actually executing it |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;db type&gt;</code> arguments are required for the <code>&lt;ChangeSlaveType&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* failed reading tablet %v: %v
* invalid type transition %v: %v -&gt;</code> %v


### DeleteTablet

Deletes tablet(s) from the topology.

#### Example

<pre class="command-example">DeleteTablet [-allow_master] &lt;tablet alias&gt; ...</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| allow_master | Boolean | Allows for the master tablet of a shard to be deleted. Use with caution. |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument must be used to specify at least one tablet when calling the <code>&lt;DeleteTablet&gt;</code> command This error occurs if the command is not called with at least one argument.


### ExecuteFetchAsDba

Runs the given SQL command as a DBA on the remote tablet.

#### Example

<pre class="command-example">ExecuteFetchAsDba [-max_rows=10000] [-disable_binlogs] [-json] &lt;tablet alias&gt; &lt;sql command&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| disable_binlogs | Boolean | Disables writing to binlogs during the query |
| json | Boolean | Output JSON instead of human-readable table |
| max_rows | Int | Specifies the maximum number of rows to allow in reset |
| reload_schema | Boolean | Indicates whether the tablet schema will be reloaded after executing the SQL command. The default value is <code>false</code>, which indicates that the tablet schema will not be reloaded. |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;sql command&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;sql command&gt;</code> arguments are required for the <code>&lt;ExecuteFetchAsDba&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### ExecuteHook

Runs the specified hook on the given tablet. A hook is a script that resides in the $VTROOT/vthook directory. You can put any script into that directory and use this command to run that script.<br><br>For this command, the param=value arguments are parameters that the command passes to the specified hook.

#### Example

<pre class="command-example">ExecuteHook &lt;tablet alias&gt; &lt;hook name&gt; [&lt;param1=value1&gt; &lt;param2=value2&gt; ...]</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;hook name&gt;</code> &ndash; Required.
* <code>&lt;param1=value1&gt;</code> <code>&lt;param2=value2&gt;</code> . &ndash; Optional.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;hook name&gt;</code> arguments are required for the <code>&lt;ExecuteHook&gt;</code> command This error occurs if the command is not called with at least 2 arguments.


### GetTablet

Outputs a JSON structure that contains information about the Tablet.

#### Example

<pre class="command-example">GetTablet &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;GetTablet&gt;</code> command This error occurs if the command is not called with exactly one argument.


### IgnoreHealthError

Sets the regexp for health check errors to ignore on the specified tablet. The pattern has implicit ^$ anchors. Set to empty string or restart vttablet to stop ignoring anything.

#### Example

<pre class="command-example">IgnoreHealthError &lt;tablet alias&gt; &lt;ignore regexp&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;ignore regexp&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;ignore regexp&gt;</code> arguments are required for the <code>&lt;IgnoreHealthError&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### InitTablet

Initializes a tablet in the topology.<br><br>

#### Example

<pre class="command-example">InitTablet [-allow_update] [-allow_different_shard] [-allow_master_override] [-parent] [-db_name_override=&lt;db name&gt;] [-hostname=&lt;hostname&gt;] [-mysql_port=&lt;port&gt;] [-port=&lt;port&gt;] [-grpc_port=&lt;port&gt;] -keyspace=&lt;keyspace&gt; -shard=&lt;shard&gt; &lt;tablet alias&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| allow_master_override | Boolean | Use this flag to force initialization if a tablet is created as master, and a master for the keyspace/shard already exists. Use with caution. |
| allow_update | Boolean | Use this flag to force initialization if a tablet with the same name already exists. Use with caution. |
| db_name_override | string | Overrides the name of the database that the vttablet uses |
| grpc_port | Int | The gRPC port for the vttablet process |
| hostname | string | The server on which the tablet is running |
| keyspace | string | The keyspace to which this tablet belongs |
| mysql_host | string | The mysql host for the mysql server |
| mysql_port | Int | The mysql port for the mysql server |
| parent | Boolean | Creates the parent shard and keyspace if they don't yet exist |
| port | Int | The main port for the vttablet process |
| shard | string | The shard to which this tablet belongs |
| tags | string | A comma-separated list of key:value pairs that are used to tag the tablet |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A slaved copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A slaved copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A slaved copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A slaved copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>schema_apply</code> &ndash; A slaved copy of data that had been serving query traffic but that is now applying a schema change. Following the change, the tablet will revert to its serving type.
    * <code>snapshot_source</code> &ndash; A slaved copy of data where mysqld is <b>not</b> running and where Vitess is serving data files to clone slaves. Use this command to enter this mode: <pre>vtctl Snapshot -server-mode ...</pre> Use this command to exit this mode: <pre>vtctl SnapshotSourceEnd ...</pre>
    * <code>spare</code> &ndash; A slaved copy of data that is ready but not serving query traffic. The data could be a potential master tablet.




#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are both required for the <code>&lt;InitTablet&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### Ping

Checks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations.

#### Example

<pre class="command-example">Ping &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;Ping&gt;</code> command This error occurs if the command is not called with exactly one argument.


### RefreshState

Reloads the tablet record on the specified tablet.

#### Example

<pre class="command-example">RefreshState &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;RefreshState&gt;</code> command This error occurs if the command is not called with exactly one argument.


### RefreshStateByShard

Runs 'RefreshState' on all tablets in the given shard.

#### Example

<pre class="command-example">RefreshStateByShard [-cells=c1,c2,...] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells whose tablets are included. If empty, all cells are considered. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitepace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;RefreshStateByShard&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ReparentTablet

Reparent a tablet to the current master in the shard. This only works if the current slave position matches the last known reparent action.

#### Example

<pre class="command-example">ReparentTablet &lt;tablet alias&gt;</pre>

#### Errors

* action <code>&lt;ReparentTablet&gt;</code> requires <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly one argument.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)


### RestoreFromBackup

Stops mysqld and restores the data from the latest backup.

#### Example

<pre class="command-example">RestoreFromBackup &lt;tablet alias&gt;</pre>

#### Errors

* the <code>&lt;RestoreFromBackup&gt;</code> command requires the <code>&lt;tablet alias&gt;</code> argument This error occurs if the command is not called with exactly one argument.


### RunHealthCheck

Runs a health check on a remote tablet.

#### Example

<pre class="command-example">RunHealthCheck &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;RunHealthCheck&gt;</code> command This error occurs if the command is not called with exactly one argument.


### SetReadOnly

Sets the tablet as read-only.

#### Example

<pre class="command-example">SetReadOnly &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;SetReadOnly&gt;</code> command This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v


### SetReadWrite

Sets the tablet as read-write.

#### Example

<pre class="command-example">SetReadWrite &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;SetReadWrite&gt;</code> command This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v


### Sleep

Blocks the action queue on the specified tablet for the specified amount of time. This is typically used for testing.

#### Example

<pre class="command-example">Sleep &lt;tablet alias&gt; &lt;duration&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;duration&gt;</code> &ndash; Required. The amount of time that the action queue should be blocked. The value is a string that contains a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms" or "1h45m". See the definition of the Go language's <a href="http://golang.org/pkg/time/#ParseDuration">ParseDuration</a> function for more details. Note that, in practice, the value should be a positively signed value.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;duration&gt;</code> arguments are required for the <code>&lt;Sleep&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### StartSlave

Starts replication on the specified slave.

#### Example

<pre class="command-example">StartSlave &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* action <code>&lt;StartSlave&gt;</code> requires <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v


### StopSlave

Stops replication on the specified slave.

#### Example

<pre class="command-example">StopSlave &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* action <code>&lt;StopSlave&gt;</code> requires <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v


### UpdateTabletAddrs

Updates the IP address and port numbers of a tablet.

#### Example

<pre class="command-example">UpdateTabletAddrs [-hostname &lt;hostname&gt;] [-ip-addr &lt;ip addr&gt;] [-mysql-port &lt;mysql port&gt;] [-vt-port &lt;vt port&gt;] [-grpc-port &lt;grpc port&gt;] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| grpc-port | Int | The gRPC port for the vttablet process |
| hostname | string | The fully qualified host name of the server on which the tablet is running. |
| mysql-port | Int | The mysql port for the mysql daemon |
| mysql_host | string | The mysql host for the mysql server |
| vt-port | Int | The main port for the vttablet process |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;UpdateTabletAddrs&gt;</code> command This error occurs if the command is not called with exactly one argument.


## Topo

* [TopoCat](#topocat)

### TopoCat

Retrieves the file(s) at &lt;path&gt; from the topo service, and displays it. It can resolve wildcards, and decode the proto-encoded data.

#### Example

<pre class="command-example">TopoCat [-cell &lt;cell&gt;] [-decode_proto] [-long] &lt;path&gt; [&lt;path&gt;...]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cell | string | topology cell to cat the file from. Defaults to global cell. |
| decode_proto | Boolean | decode proto files and display them as text |
| long | Boolean | long listing. |


#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;path&gt;</code> &ndash; Required.
* <code>&lt;path&gt;</code>. &ndash; Optional.

#### Errors

* <code>&lt;TopoCat&gt;</code>: no path specified This error occurs if the command is not called with at least one argument.
* <code>&lt;TopoCat&gt;</code>: invalid wildcards: %v
* <code>&lt;TopoCat&gt;</code>: some paths had errors


## Workflows

* [WorkflowAction](#workflowaction)
* [WorkflowCreate](#workflowcreate)
* [WorkflowDelete](#workflowdelete)
* [WorkflowStart](#workflowstart)
* [WorkflowStop](#workflowstop)
* [WorkflowTree](#workflowtree)
* [WorkflowWait](#workflowwait)

### WorkflowAction

Sends the provided action name on the specified path.

#### Example

<pre class="command-example">WorkflowAction &lt;path&gt; &lt;name&gt;</pre>

#### Arguments

* <code>&lt;name&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;path&gt;</code> and <code>&lt;name&gt;</code> arguments are required for the <code>&lt;WorkflowAction&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* no workflow.Manager registered


### WorkflowCreate

Creates the workflow with the provided parameters. The workflow is also started, unless -skip_start is specified.

#### Example

<pre class="command-example">WorkflowCreate [-skip_start] &lt;factoryName&gt; [parameters...]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| skip_start | Boolean | If set, the workflow will not be started. |


#### Arguments

* <code>&lt;factoryName&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;factoryName&gt;</code> argument is required for the <code>&lt;WorkflowCreate&gt;</code> command This error occurs if the command is not called with at least one argument.
* no workflow.Manager registered


### WorkflowDelete

Deletes the finished or not started workflow.

#### Example

<pre class="command-example">WorkflowDelete &lt;uuid&gt;</pre>

#### Errors

* the <code>&lt;uuid&gt;</code> argument is required for the <code>&lt;WorkflowDelete&gt;</code> command This error occurs if the command is not called with exactly one argument.
* no workflow.Manager registered


### WorkflowStart

Starts the workflow.

#### Example

<pre class="command-example">WorkflowStart &lt;uuid&gt;</pre>

#### Errors

* the <code>&lt;uuid&gt;</code> argument is required for the <code>&lt;WorkflowStart&gt;</code> command This error occurs if the command is not called with exactly one argument.
* no workflow.Manager registered


### WorkflowStop

Stops the workflow.

#### Example

<pre class="command-example">WorkflowStop &lt;uuid&gt;</pre>

#### Errors

* the <code>&lt;uuid&gt;</code> argument is required for the <code>&lt;WorkflowStop&gt;</code> command This error occurs if the command is not called with exactly one argument.
* no workflow.Manager registered


### WorkflowTree

Displays a JSON representation of the workflow tree.

#### Example

<pre class="command-example">WorkflowTree </pre>

#### Errors

* the <code>&lt;WorkflowTree&gt;</code> command takes no parameter This error occurs if the command is not called with exactly 0 arguments.
* no workflow.Manager registered


### WorkflowWait

Waits for the workflow to finish.

#### Example

<pre class="command-example">WorkflowWait &lt;uuid&gt;</pre>

#### Errors

* the <code>&lt;uuid&gt;</code> argument is required for the <code>&lt;WorkflowWait&gt;</code> command This error occurs if the command is not called with exactly one argument.
* no workflow.Manager registered


