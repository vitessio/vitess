local config = import '../../../config.libsonnet';
local configuration_templates = import './configuration_templates.libsonnet';

{
  // TODO: add description for each panel.
  panels: {

    local panel_template = configuration_templates.prometheus_vitess.panel.mysql_default,
    local keyspace_overview_panel_template = configuration_templates.prometheus_vitess.panel.vitess_keyspace_overview,
    local vttablet_host_view_panel_template = configuration_templates.prometheus_vitess.panel.vitess_vttablet_host_view,

    // TODO Create a recording rule for the prometheus target.
    mysqlRestart: panel_template {
      title: 'MySQL (by keyspace/shard)',
      bars: true,
      format: 'short',
      legend_sort: 'max',
      legend_sortDesc: false,
      lines: false,
      sort: 'increasing',
      targets: [
        {
          expr: |||
            count by (keyspace, shard) (
              idelta (
                mysql_global_status_uptime{
                  %(mysqlSelector)s
                }[5m]
              ) < 0
            )
          ||| % config._config,
          legendFormat: '{{keyspace}}/{{shard}}',
        },
      ],
    },

    // TODO Create a recording rule for the prometheus target.
    mysqlSlowQueriesByKeyspaceShard: panel_template {
      title: 'Slow queries (by keyspace/shard)',
      format: 'cps',
      legend_min: true,
      legend_current: true,
      legend_sort: 'current',
      legend_sortDesc: true,
      sort: 'decreasing',
      nullPointMode: 'null as zero',
      targets: [
        {
          expr: |||
            sum by (keyspace, shard)(
              rate(
                mysql_global_status_slow_queries{
                  %(mysqlSelector)s
                }[$interval]
              )
            ) > 0
          ||| % config._config,
          legendFormat: '{{keyspace}}/{{shard}}',
        },
      ],
    },

    // TODO Create a recording rule for the prometheus target.
    mysqlSlowQueriesByKeyspace: keyspace_overview_panel_template {
      title: 'Slow Queries',
      format: 'ops',
      legend_avg: true,
      targets: [
        {
          expr: |||
            sum by(keyspace)(
              vitess_mixin:mysql_global_status_slow_queries_by_keyspace_shard:irate1m{
                keyspace="$keyspace"
              }
            )
          |||,
          legendFormat: '{{keyspace}}',
          intervalFactor: 1,
        },
      ],
    },

    // TODO Create a recording rule for the prometheus target.
    mysqlSlowQueriesByInstanceFilteredByShardKeyspace: vttablet_host_view_panel_template {
      title: 'Slow Queries',
      format: 'ops',
      targets: [
        {
          expr: |||
            sum by(instance)(
              rate(
                mysql_global_status_slow_queries{
                  keyspace="$keyspace",
                  shard=~"$shard",
                  instance=~"$host"
                }[5m]
              )
            )
          |||,
          legendFormat: '{{instance}}',
          intervalFactor: 1,
        },
      ],
    },

    // TODO Create a recording rule for the prometheus target.
    mysqlReplicationLagByKeyspace: keyspace_overview_panel_template {
      title: 'Replication Lag',
      format: 's',
      targets: [
        {
          expr: |||
            sum by (keyspace)(
              mysql_slave_status_seconds_behind_master{
                keyspace="$keyspace"
              }
            ) > 1
          ||| % config._config,
          legendFormat: '{{keyspace}}',
          intervalFactor: 1,
        },
      ],
    },

    // TODO Create a recording rule for the prometheus target.
    mysqlInnoDBRowsReadOperationsByKeyspace: keyspace_overview_panel_template {
      title: 'InnoDB row read operations',
      targets: [
        {
          expr: |||
            sum by (keyspace)(
              rate(
                mysql_global_status_innodb_row_ops_total{
                  keyspace="$keyspace",
                  operation="read"
                }[5m]
              )
            )
          ||| % config._config,
          legendFormat: '{{keyspace}}',
          intervalFactor: 1,
        },
      ],
    },

    // TODO Create a recording rule for the prometheus target.
    mysqlSemiSyncAvgWaitByKeyspace: keyspace_overview_panel_template {
      title: 'Semi-sync avg wait',
      format: 'µs',
      targets: [
        {
          expr: |||
            sum by (keyspace)(
              rate(
                mysql_global_status_rpl_semi_sync_master_tx_avg_wait_time{
                  keyspace="$keyspace"
                }[1m]
              )
            )
          ||| % config._config,
          legendFormat: '{{keyspace}}',
          intervalFactor: 1,
        },
      ],
    },

    // TODO Create a recording rule for the prometheus target.
    mysqlSemiSyncAvgWaitByKeyspaceFilteredByInstanceShard: vttablet_host_view_panel_template {
      title: 'Semi-sync avg wait',
      format: 'µs',
      targets: [
        {
          expr: |||
            sum by (keyspace)(
              rate(
                mysql_global_status_rpl_semi_sync_master_tx_avg_wait_time{
                  keyspace="$keyspace",
                  shard=~"$shard",
                  instance=~"$host"
                }[5m]
              )
            )
          ||| % config._config,
          legendFormat: '{{keyspace}}',
          intervalFactor: 1,
        },
      ],
    },

    mysqlVersionByKeyspace: keyspace_overview_panel_template {
      title: 'MySQL version',
      targets: [
        {
          expr: |||
            sum by (keyspace, version)(
              mysql_version_info{
                keyspace="$keyspace"
              }
            ) 
          ||| % config._config,
          legendFormat: '{{keyspace}}/{{version}}',
          intervalFactor: 1,
        },
      ],
    },
  },
}
