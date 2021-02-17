// Re-cyclable components for template resources
local config = import '../../../config.libsonnet';
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';

local template = grafana.template;
{
  interval:: template.interval(
    name='interval',
    label='Interval',
    query='auto,1m,5m,10m,30m,1h,6h,12h',
    current='auto',
    auto_min='1m'
  ),

  keyspace::
    template.new(
      'keyspace',
      '%(dataSource)s' % config._config,
      'query_result(sum by(keyspace)(vttablet_build_number{%(vttabletSelector)s}))',
      regex='.*keyspace="(.*)".*',
      label='Keyspace',
      refresh='load',
      includeAll=false,
      sort=1,
    ),

  table::
    template.new(
      'table',
      '%(dataSource)s' % config._config,
      'query_result(sum by(table)(vitess_mixin:vtgate_queries_processed_by_table:irate1m{keyspace="$keyspace"}))',
      regex='.*table="(.*)".*',
      label='Table',
      refresh='time',
      includeAll=true,
      sort=1,
      allValues='.*',
    ),

  host::
    template.new(
      'host',
      '%(dataSource)s' % config._config,
      'label_values(vtgate_build_number{ instance)',
      regex='',
      label='Host(s)',
      refresh='time',
      includeAll=true,
      multi=true,
      allValues='.*',
    ),

  hostByKeyspaceShard::
    template.new(
      'host',
      '%(dataSource)s' % config._config,
      'label_values(vttablet_build_number{keyspace="$keyspace", shard=~"$shard"}, instance)',
      regex='',
      label='Host(s)',
      refresh='time',
      includeAll=true,
      multi=true,
      allValues='.*',
      sort=1
    ),

  shard::
    template.new(
      'shard',
      '%(dataSource)s' % config._config,
      'label_values(vttablet_build_number{keyspace="$keyspace"}, shard)',
      regex='',
      label='Shard',
      refresh='time',
      includeAll=true,
      sort=1,
      allValues='.*'
    ),

  shard_multi::
    template.new(
      'shard',
      '%(dataSource)s' % config._config,
      'label_values(vttablet_build_number{keyspace="$keyspace"}, shard)',
      regex='',
      label='Shard',
      refresh='time',
      multi=true,
      includeAll=false,
      sort=1,
      allValues='.*'
    ),
}
