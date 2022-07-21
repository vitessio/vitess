// Re-cyclable components for singlestat resources
local config = import '../../../config.libsonnet';
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';

local singlestat = grafana.singlestat;
local prometheus = grafana.prometheus;

{
  //TODO move to resource to use vtgate_config/vtgate_helper
  vtgateSuccessRate::
    singlestat.new(
      'Query success - vtgate',
      datasource='%(dataSource)s' % config._config,
      colorBackground=true,
      decimals=4,
      format='percent',
      colors=[
        '#d44a3a',
        'rgba(237, 129, 40, 0.89)',
        '#299c46',
      ],
      valueFontSize='70%',
      valueName='current',
      thresholds='0.99,0.999',
    )
    .addTarget(
      prometheus.target(
        |||
          100 
          -
          sum(
            rate(
              vtgate_api_error_counts{
                %(vtgateSelector)s
              }[$interval]
            ) OR vector(0)
          )
          /
          sum(
            rate(
              vtgate_api_count{
                %(vtgateSelector)s
              }[$interval]
            )
          )
        ||| % config._config,
        instant=true,
        intervalFactor=1
      )
    ),

  //TODO move to resource to use vtgate_config/vtgate_helper
  vtgateUp::
    singlestat.new(
      'vtgate',
      datasource='%(dataSource)s' % config._config,
      valueFontSize='50%',
      valueName='current',
    )
    .addTarget(
      prometheus.target(
        |||
          sum(
            up{
              %(vtgateSelector)s
            }
          )
        ||| % config._config,
        instant=true,
        intervalFactor=1
      )
    ),

  //TODO move to resource to use vttablet_config/vttablet_helper
  vttabletQuerySuccess::
    singlestat.new(
      'Query success - vttablet',
      datasource='%(dataSource)s' % config._config,
      colorBackground=true,
      decimals=4,
      format='percent',
      colors=[
        '#d44a3a',
        'rgba(237, 129, 40, 0.89)',
        '#299c46',
      ],
      valueFontSize='70%',
      valueName='current',
      thresholds='0.99,0.999',
    )
    .addTarget(
      prometheus.target(
        |||
          100 
          -
          (
            sum (
              vitess_mixin:vttablet_errors:rate1m
            )
            /
            sum (
              vitess_mixin:vttablet_query_counts:rate1m
            )
          )
        ||| % config._config,
        instant=true,
        intervalFactor=1
      )
    ),

  //TODO move to resource to use vttablet_config/vttablet_helper
  vttabletUp::
    singlestat.new(
      'vttablet',
      datasource='%(dataSource)s' % config._config,
      valueFontSize='50%',
      valueName='current',
    )
    .addTarget(
      prometheus.target(
        |||
          sum(
            up{
              %(vttabletSelector)s
            }
          )
        ||| % config._config,
        instant=true,
        intervalFactor=1
      )
    ),


  //TODO move to resource to use vttablet_config/vttablet_helper
  keyspaceCount::
    singlestat.new(
      'keyspace',
      description='count of keyspaces with active queries',
      datasource='%(dataSource)s' % config._config,
      valueFontSize='50%',
      valueName='current',
    )
    .addTarget(
      prometheus.target(
        |||
          count(
            count by (keyspace)(
              vtgate_vttablet_call_count{
              }
            )
          )
        ||| % config._config,
        instant=true,
        intervalFactor=1
      )
    ),

  //TODO move to resource to use vttablet_config/vttablet_helper
  shardCount::
    singlestat.new(
      'shard',
      datasource='%(dataSource)s' % config._config,
      valueFontSize='50%',
      valueName='current',
    )
    .addTarget(
      prometheus.target(
        |||
          count(
            count by(shard)(
              vttablet_tablet_state{
                %(vttabletSelector)s
              }
            )
          )
        ||| % config._config,
        instant=true,
        intervalFactor=1
      )
    ),

  mysqlQPS::
    singlestat.new(
      'QPS - MySQL',
      datasource='%(dataSource)s' % config._config,
      format='short',
      valueFontSize='70%',
      valueName='current',
      sparklineFull=true,
      sparklineShow=true,
    )
    .addTarget(
      prometheus.target(
        |||
          sum (
            vitess_mixin:mysql_global_status_queries:rate1m
          )
        |||,
        intervalFactor=1
      )
    ),

  vtctldUp::
    singlestat.new(
      'vtctld',
      datasource='%(dataSource)s' % config._config,
      valueFontSize='50%',
      valueName='current',
    )
    .addTarget(
      prometheus.target(
        |||
          sum(
            up{
              %(vtctldSelector)s})
        ||| % config._config,
        instant=true,
        intervalFactor=1
      )
    ),
}
