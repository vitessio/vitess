// Re-cyclable components for heatmap resources
local config = import '../../../config.libsonnet';
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';

local heatmap = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
{
  //TODO move to vttablet_config.libsonnet
  vttabletQueryDuration::
    heatmap.new(
      'Query duration heatmap',
      datasource='%(dataSource)s' % config._config,
      dataFormat='tsbuckets',
      yAxis_format='s',
      color_cardColor='#FF9830',
      color_exponent=0.3,
      color_mode='opacity',
      yAxis_decimals=0,
    )
    .addTarget(
      prometheus.target(
        |||
          sum  by (le)(
            vitess_mixin:vttablet_queries_bucket_byregion_keyspace:rate1m{
              %(customCommonSelector)s,
              keyspace="$keyspace"
            }
          )
        ||| % config._config,
        format='heatmap',
        legendFormat='{{le}}'
      )
    ),

  //TODO move to vttablet_config.libsonnet
  vttabletTransactionDuration::
    heatmap.new(
      'Transaction duration heatmap',
      datasource='%(dataSource)s' % config._config,
      dataFormat='tsbuckets',
      yAxis_format='s',
      color_cardColor='#FF9830',
      color_exponent=0.3,
      color_mode='opacity',
      yAxis_decimals=0,
    )
    .addTarget(
      prometheus.target(
        |||
          sum by (le)(
            vitess_mixin:vttablet_transactions_bucket_byregion_keyspace:rate1m{
              %(customCommonSelector)s,
              keyspace="$keyspace"
            }
          )
        ||| % config._config,
        format='heatmap',
        legendFormat='{{le}}'
      )
    ),

  //TODO move to resources/vttablet
  //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
  vttabletQueryTimeDistribution::
    heatmap.new(
      title='Query Time Distribution (Heatmap)',
      description='Shows a heatmap of the histogram bucketing of the time per read query.',
      datasource='%(dataSource)s' % config._config,
      dataFormat='tsbuckets',
      yAxis_format='s',
      color_cardColor='#FF9830',
      color_exponent=0.3,
      color_mode='opacity',
      yAxis_decimals=0,
    ).addTarget(
      prometheus.target(
        |||
          sum by (le) (
            rate(
              vttablet_queries_bucket{
              %(customCommonSelector)s,
                keyspace=~"$keyspace",
                shard=~"$shard",
                instance=~"$host"
                }[1m]
              )
            )
        ||| % config._config,
        format='heatmap',
        legendFormat='{{le}}'
      )
    ),
}
