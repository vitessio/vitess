// Re-cyclable components for row resources
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';
local row = grafana.row;

//TODO move all rows to config/row_config.libsonnet and update the layouts to use grafonnet_helper.getRow()
{
  connection::
    row.new(
      title='Connection',
    ),

  cpu::
    row.new(
      title='CPU',
      collapse=true,
    ),

  duration::
    row.new(
      title='Duration',
      collapse=true,
    ),

  errorsRowsReturned::
    row.new(
      title='Errors / Rows returned',
    ),

  errors::
    row.new(
      title='Errors',
      collapse=true,
    ),

  healthcheck::
    row.new(
      title='Healthcheck',
      collapse=true,
    ),

  tabletsQueries::
    row.new(
      title='Tablets/Queries',
    ),

  mysql::
    row.new(
      title='MySQL',
      collapse=true,
    ),

  misc::
    row.new(
      title='Misc',
      collapse=true,
    ),

  networkingTCP::
    row.new(
      title='Networking TCP',
      collapse=true,
    ),

  networkNIC::
    row.new(
      title='Network NIC',
      collapse=true,
    ),

  OS::
    row.new(
      title='OS',
      collapse=true,
    ),

  processes::
    row.new(
      title='Processes',
      collapse=true,
    ),

  queryTimings::
    row.new(
      // as we don't have timings by table (yet!)
      title="Query/Transaction timings (table filter doesn't apply)",
      collapse=true,
    ),

  query::
    row.new(
      title='Query',
    ),

  RED::
    row.new(
      title='RED - Requests / Error rate / Duration',
    ),

  REDByKeyspace::
    row.new(
      title='RED (by keyspace)',
      collapse=true
    ),

  REDByTabletType::
    row.new(
      title='RED (by tablet type)',
      collapse=true
    ),

  REDByPlanType::
    row.new(
      title='RED (by plan type)',
      collapse=true
    ),

  REDByShard::
    row.new(
      title='RED (by shard)',
      collapse=true
    ),

  REDByTable::
    row.new(
      title='RED (by table)',
      collapse=true
    ),


  rowsReturned::
    row.new(
      title='Rows returned',
      collapse=true,
    ),

  serviceRestart::
    row.new(
      title='Service restart',
    ),

  storage::
    row.new(
      title='Storage',
      collapse=true,
    ),

  topLevel::
    row.new(
      title='Top level',
    ),

  topologyWatcher::
    row.new(
      title='Topology watcher',
      collapse=true,
    ),

  vitessQueryPool::
    row.new(
      title='Vitess - Query pool',
      collapse=true,
    ),

  vitessTransactionPool::
    row.new(
      title='Vitess - Transaction pool',
      collapse=true,
    ),

  vtgate::
    row.new(
      title='vtgate - Requests (by table / by plan / by keyspace )',
      collapse=true,
    ),

  vttablet::
    row.new(
      title='vttablet - Requests (by table / by plan / by keyspace )',
      collapse=true,
    ),

}
