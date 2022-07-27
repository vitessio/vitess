local config = import '../../../config.libsonnet';
local configuration_templates = import './configuration_templates.libsonnet';
local vitess_ct = configuration_templates.prometheus_vitess;

// TODO: move local template variables and fields to ./configuration_templates.libsonnet.
{
  // TODO: add description for each panel.
  panels: {

    local panel_template = vitess_ct.panel.mysql_default,
    local vttablet_host_view_panel_template = vitess_ct.panel.vitess_vttablet_host_view,

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
    mysqlSlowQueries: panel_template
                      + vitess_ct.panel.null_as_zeros {
                        title: 'Slow queries',
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
                              sum (
                                rate(
                                  mysql_global_status_slow_queries{
                                    %(mysqlSelector)s
                                  }[$interval]
                                )
                              )
                            ||| % config._config,
                            legendFormat: 'Slow Queries',
                          },
                        ],
                      },

    // TODO Create a recording rule for the prometheus target.
    mysqlSlowQueriesByInstance: vttablet_host_view_panel_template
                                + vitess_ct.panel.null_as_zeros {
                                  title: 'Slow Queries',
                                  format: 'ops',
                                  targets: [
                                    {
                                      expr: |||
                                        sum by(instance)(
                                          rate(
                                            mysql_global_status_slow_queries{
                                              instance=~"$host"
                                            }[1m]
                                          )
                                        )
                                      |||,
                                      legendFormat: '{{instance}}',
                                      intervalFactor: 1,
                                    },
                                  ],
                                },
  },
}
