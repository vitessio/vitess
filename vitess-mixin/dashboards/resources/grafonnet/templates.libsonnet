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

  table::
    template.new(
      'table',
      '%(dataSource)s' % config._config,
      'query_result(sum by(table)(vitess_mixin:vtgate_queries_processed_by_table:rate1m{keyspace="$keyspace"}))',
      regex='.*table="(.*)".*',
      label='Table',
      refresh='time',
      includeAll=true,
      sort=1,
      allValues='.*',
    ),

  hostVtgate::
    template.new(
      'host',
      '%(dataSource)s' % config._config,
      'label_values(vtgate_build_number, instance)',
      label='Host(s)',
      refresh='time',
      multi=true,
      allValues='.*',
    ),

  hostVttablet::
    template.new(
      'host',
      '%(dataSource)s' % config._config,
      'label_values(vttablet_build_number{}, instance)',
      label='Host(s)',
      refresh='time',
      multi=true,
      allValues='.*',
      sort=1
    ),

}
