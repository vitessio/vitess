// Re-cyclable components for template resources
local config = import '../../../config.libsonnet';
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';

local template = grafana.template;
{
  region:: template.new(
    'region',
    '%(dataSource)s' % config._config,
    'label_values(vtctld_build_number{%(vtctldSelector)s}, region)' % config._config,
    label='Region',
    refresh='time',
    includeAll=false,
    sort=1,
  ),

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
      'query_result(sum by(keyspace)(vttablet_build_number{%(customCommonSelector)s, %(vttabletSelector)s}))' % config._config,
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
      'query_result(sum by(table)(vitess_mixin:vtgate_queries_processed_by_table:irate1m{%(customCommonSelector)s,keyspace="$keyspace"}))' % config._config,
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
      'label_values(vtgate_build_number{%(customCommonSelector)s}, instance)' % config._config,
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
      'label_values(vttablet_build_number{%(customCommonSelector)s, keyspace="$keyspace", shard=~"$shard"}, instance)' % config._config,
      regex='',
      label='Host(s)',
      refresh='time',
      includeAll=true,
      multi=true,
      allValues='.*',
      sort=1
    ),

  hostByCurrentOSRelease::
    template.new(
      'host%(currentOSLabel)s' % config._config,
      '%(nodeDataSource)s' % config._config,
      'label_values(node_uname_info{instance=~".*tablet.*",release=~"%(currentOSRelease)s.*"}, instance)' % config._config,
      regex='',
      label='Host(s) %(currentOSLabel)s' % config._config,
      refresh='load',
      multi=true,
      includeAll=false,
      allValues='.*',
      sort=1
    ),
  hostByNextOSRelease::
    template.new(
      'host%(nextOSLabel)s' % config._config,
      '%(nodeDataSource)s' % config._config,
      'label_values(node_uname_info{instance=~".*tablet.*",release=~"%(nextOSRelease)s.*"}, instance)' % config._config,
      regex='',
      refresh='load',
      label='Host(s) %(nextOSLabel)s' % config._config,
      multi=true,
      includeAll=false,
      allValues='.*',
      sort=1
    ),

  shard::
    template.new(
      'shard',
      '%(dataSource)s' % config._config,
      'label_values(vttablet_build_number{%(customCommonSelector)s, keyspace="$keyspace"}, shard)' % config._config,
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
      'label_values(vttablet_build_number{%(customCommonSelector)s, keyspace="$keyspace"}, shard)' % config._config,
      regex='',
      label='Shard',
      refresh='time',
      multi=true,
      includeAll=false,
      sort=1,
      allValues='.*'
    ),
}
