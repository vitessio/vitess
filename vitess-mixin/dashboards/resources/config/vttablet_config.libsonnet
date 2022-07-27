/** This is a configuration file containing metadata for vttablet grafana resources. */

local config = import '../../../config.libsonnet';
local configuration_templates = import './configuration_templates.libsonnet';
local vitess_ct = configuration_templates.prometheus_vitess;

// TODO: move local template variables to ./configurations_templates.libsonnet.
{
  panels: {
    //  ____                  _
    // |  _ \ __ _ _ __   ___| |___
    // | |_) / _` | '_ \ / _ \ / __|
    // |  __/ (_| | | | |  __/ \__ \
    // |_|   \__,_|_| |_|\___|_|___/

    // TODO: add description for each panel.

    //Override default_panel values with custom configuration
    local vttablet_queries_killed = vitess_ct.panel.legend_min_max_avg + vitess_ct.panel.null_as_zeros,
    local vttablet_query_errors_by_type = vitess_ct.panel.legend_min_max_avg + vitess_ct.panel.null_as_zeros,

    local panel_template = vitess_ct.panel.legend_min_max_current {
      legend_sort: 'current',
      legend_sortDesc: true,
      shared_tooltip: true,
      sort: 'decreasing',
    },

    local vttablet_host_view_panel_template = panel_template {
      legend_sort: 'avg',
      legend_avg: true,
      legend_current: false,
    },

    //TODO Create a recording rule.
    countServingTablets:
      panel_template {
        title: '# of serving tablets',
        legend_sortDesc: false,
        shared_tooltip: false,
        sort: 'increasing',
        targets: [
          {
            expr:
              |||
                count(
                  vttablet_tablet_server_state{
                    %(vttabletSelector)s,
                    name="SERVING"
                  }
                )
              |||
              % config._config,
            legendFormat: 'SERVING',
          },
        ],
      },

    vttabletRequestsByTable:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests (by table)',
        format: 'rps',
        targets: [
          {
            expr:
              |||
                sum by (table)(
                  vitess_mixin:vttablet_query_counts_by_keyspace_table:rate1m{
                    table=~"$table"
                  }
                )
                or
                vector(0)
              |||,
            legendFormat: '{{table}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMEHTEUS TARGET
    vttabletRequestsByPlanType:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests (by plan type)',
        format: 'ops',
        nullPointMode: 'null as zero',
        targets: [
          {
            expr:
              |||
                sum by (plan_type)(
                  rate(
                    vttablet_queries_count{
                      instance=~"$host"
                    } [1m]
                  )
                )
              |||,
            legendFormat: '{{plan_type}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletRequestsByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests',
        format: 'ops',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'avg',
        nullPointMode: 'null as zero',
        targets: [
          {
            expr:
              |||
                sum  by (instance)(
                  rate(
                    vttablet_query_counts{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletRequestsByTableFilteredByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests (by table)',
        format: 'ops',
        nullPointMode: 'null as zero',
        targets: [
          {
            expr:
              |||
                sum  by (table)(
                  rate(
                    vttablet_query_counts{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{table}}',
            intervalFactor: 1,
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletErrorRateByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate',
        format: 'percentunit',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'avg',
        targets: [
          {
            expr:
              |||
                sum by (instance)(
                  rate(
                    vttablet_query_error_counts{
                      instance=~"$host"
                    }[1m]
                  )
                )
                /
                (
                  sum by (instance)(
                    rate(
                      vttablet_query_error_counts{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                  +
                  sum by (instance)(
                    rate(
                      vttablet_query_counts{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    vttabletErrorRateByPlanFilteredByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate (by plan type)',
        format: 'percentunit',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'avg',
        targets: [
          {
            expr:
              |||
                sum by (plan)(
                  rate(
                    vttablet_query_error_counts{
                      instance=~"$host"
                    }[1m]
                  )
                )
                /
                (
                  sum by (plan)(
                    rate(
                      vttablet_query_error_counts{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                  +
                  sum by (plan)(
                    rate(
                      vttablet_query_counts{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{plan}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletErrorRateByTableFilteredByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate (by table)',
        format: 'percentunit',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'avg',
        targets: [
          {
            expr:
              |||
                sum by (table)(
                  rate(
                    vttablet_query_error_counts{
                      instance=~"$host"
                    }[1m]
                  )
                )
                /
                (
                  sum by (table)(
                    rate(
                      vttablet_query_error_counts{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                  +
                  sum by (table)(
                    rate(
                      vttablet_query_counts{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{table}}',
          },
        ],
      },

    vttabletRowsReturnedByTableFilteredByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Rows Returned (by table)',
        targets: [
          {
            expr:
              |||
                sum by (table) (
                  rate(
                    vttablet_query_row_counts{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{table}}',
          },
        ],
      },

    vttabletRowsReturnedByPlansFilterByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Rows Returned (by plan)',
        targets: [
          {
            expr:
              |||
                sum by (plan) (
                  rate(
                    vttablet_query_row_counts{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{plan}}',
          },
        ],
      },

    //TODO DEDUPLICATE LEGEND CONFIGURATION FOR QUERY DURATION PANELS
    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationAvgByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Query Duration (avg)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'max',
        targets: [
          {
            expr:
              |||
                sum by(instance)(
                  rate(
                    vttablet_queries_sum{
                      instance=~"$host"
                    }[1m]
                  )
                )
                /
                sum by(instance)(
                  rate(
                    vttablet_queries_count{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP50ByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Query Duration (p50)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'max',
        targets: [
          {
            expr:
              |||
                histogram_quantile(
                  0.50,sum by(instance,le)(
                    rate(
                      vttablet_queries_bucket{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP95ByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Query Duration (p95)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'max',
        targets: [
          {
            expr:
              |||
                histogram_quantile(
                  0.95,sum by(instance,le)(
                    rate(
                      vttablet_queries_bucket{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP99ByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Duration (p99)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'avg',
        targets: [
          {
            expr:
              |||
                histogram_quantile(
                  0.99,sum by(instance,le)(
                    rate(
                      vttablet_queries_bucket{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP99ByPlan:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Duration p99 (by plan type)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'avg',
        targets: [
          {
            expr:
              |||
                histogram_quantile(
                  0.99,sum by(plan_type,le)(
                    rate(
                      vttablet_queries_bucket{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{plan_type}}',
          },
        ],
      },

    //TODO DEDUPLICATE LEGEND CONFIGURATION FOR TRANSACTION DURATION PANELS
    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionDurationAvgByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Transaction Duration (avg)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'max',
        targets: [
          {
            expr:
              |||
                sum by(instance)(
                  rate(
                    vttablet_transactions_sum{
                      instance=~"$host"
                    }[1m]
                  )
                )
                /
                sum by(instance)(
                  rate(
                    vttablet_transactions_count{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionDurationP50ByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Transaction Duration (p50)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'max',
        targets: [
          {
            expr:
              |||
                histogram_quantile(
                  0.50,sum by(instance,le)(
                    rate(
                      vttablet_transactions_bucket{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionDurationP95ByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Transaction Duration (p95)',
        format: 's',
        legend_current: false,
        legend_avg: true,
        legend_sort: 'max',
        targets: [
          {
            expr:
              |||
                histogram_quantile(
                  0.95,sum by(instance,le)(
                    rate(
                      vttablet_transactions_bucket{
                        instance=~"$host"
                      }[1m]
                    )
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    vttabletQueryTransactionKilled:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Query/Transaction killed',
        format: 'cps',
        legend_alignAsTable: true,
        shared_tooltip: false,
        targets: [
          {
            expr:
              |||
                sum (
                  vitess_mixin:vttablet_kills:rate1m
                )
              |||,
            legendFormat: 'Killed',
          },
        ],
      },

    vttabletRestart: {
      title: 'vttablet',
      bars: true,
      datasource: '%(dataSource)s' % config._config,
      fill: 0,
      format: 'short',
      legend_values: true,
      legend_alignAsTable: true,
      legend_max: true,
      legend_sort: 'max',
      legend_sortDesc: false,
      lines: false,
      min: 0,
      shared_tooltip: false,
      sort: 'increasing',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                vitess_mixin:process_start_time_seconds_by_instance_job:sum5m{
                  %(vttabletSelector)s
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolAvailableConnections: vttablet_host_view_panel_template {
      title: 'Available Connections',
      description: 'number of available connections in the pool in real-time',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                vttablet_conn_pool_available{
                  instance=~'$host'
                }
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolActiveConnections: vttablet_host_view_panel_template {
      title: 'Active Connections',
      description: 'count of in use connections to mysql',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by(instance) (
                vttablet_conn_pool_active{
                  instance=~'$host'
                }
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolIddleClosedRate: vttablet_host_view_panel_template {
      title: 'Idle Closed Rate',
      description: 'rate of closing connections due to the idle timeout',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_conn_pool_idle_closed{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolWaitCount: vttablet_host_view_panel_template {
      title: 'Wait count',
      description: 'WaitCount will give you how often the transaction pool gets full that causes new transactions to wait.',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_conn_pool_wait_count{
                    instance=~'$host'
                  }[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolAvgWaitTime: vttablet_host_view_panel_template {
      title: 'Avg wait time',
      format: 's',
      description: 'WaitTime/WaitCount will tell you the average wait time.',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_conn_pool_wait_time{
                    instance=~"$host"
                  }[1m]
                )
              )
              /
              sum by (instance) (
                rate(
                  vttablet_conn_pool_wait_count{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    vttabletQueriesKilled: vttablet_queries_killed {
      title: 'Queries Killed',
      description: |||
        Kills reports the queries and transactions killed by VTTablet due to timeout.
        It’s a very important variable to look at during outages.
      |||,
      targets: [
        {
          expr: |||
            sum by (instance)(
              vitess_mixin:vttablet_kills_by_instance:rate1m{
                instance=~"$host"
              }
            )
          |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    vttabletQueryErrorsByType: vttablet_query_errors_by_type {
      title: 'Query errors (by error code)',
      description: '',
      targets: [
        {
          expr: |||
            sum by (error_code)(
              vitess_mixin:vttablet_errors:rate1m{
                instance=~"$host"
              }
            )
          |||,
          legendFormat: 'ErrorCode: {{error_code}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolAvailableConnections: vttablet_host_view_panel_template {
      title: 'Available Connections',
      description: 'number of available connections in the pool',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                vttablet_transaction_pool_available{
                  instance=~'$host'
                }
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolActiveConnections: vttablet_host_view_panel_template {
      title: 'Active Connections',
      description: 'Number of connections actually open to mysql',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by(instance) (
                vttablet_transaction_pool_active{
                  instance=~'$host'
                }
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolIddleClosedRate: vttablet_host_view_panel_template {
      title: 'Idle Closed Rate',
      description: 'Rate of closing connections due to the idle timeout',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_transaction_pool_idle_closed{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolWaitCount: vttablet_host_view_panel_template {
      title: 'Wait count',
      description: 'WaitCount will give you how often the transaction pool gets full that causes new transactions to wait.',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_transaction_pool_wait_count{
                    instance=~'$host'
                  }[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolAvgWaitTime: vttablet_host_view_panel_template {
      title: 'Avg wait time',
      format: 's',
      description: 'WaitTime/WaitCount will tell you the average wait time.',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_transaction_pool_wait_time{
                    instance=~"$host"
                  }[1m]
                )
              )
              /
              sum by (instance) (
                rate(
                  vttablet_transaction_pool_wait_count{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletGarbageCollectionCount: vitess_ct.panel.go_gc_ops {
      title: 'GC Count',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  go_gc_duration_seconds_count{
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

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletGarbageCollectionDuration: vitess_ct.panel.go_gc_seconds {
      title: 'GC Duration total per second',
      description: 'A summary of the pause duration of garbage collection cycles',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  go_gc_duration_seconds_count{
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
    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletGarbageCollectionDurationQuantiles: vitess_ct.panel.go_gc_seconds {
      title: 'GC Duration quantiles (all hosts)',
      targets: [
        {
          expr:
            |||
              sum by(quantile)(
                rate(
                  go_gc_duration_seconds{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
          legendFormat: 'p{{quantile}}',
          intervalFactor: 1,
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletMysqlTimeAvgFilteredByInstance: vitess_ct.panel.mysql_timings {
      title: 'MySQL time (avg)',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_mysql_sum{
                    instance=~"$host"
                  }[1m]
                )
              ) 
              /
              sum by (instance) (
                rate(
                  vttablet_mysql_count{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletMysqlExecTimeP50FilterebyInstance: vitess_ct.panel.mysql_timings {
      title: 'MySQL Exec Time P50',
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.50,
              sum by (le, instance) (
                rate(
                  vttablet_mysql_bucket{
                    operation="Exec",
                    instance=~"$host"
                  }[1m]
                )
              )
            ) 
          |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletMysqlExecTimeP95FilterebyInstance: vitess_ct.panel.mysql_timings {
      title: 'MySQL Exec Time P95',
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.95,
              sum by (le, instance) (
                rate(
                  vttablet_mysql_bucket{
                    operation="Exec",
                    instance=~"$host"
                  }[1m]
                )
              )
            ) 
          |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vtgateToVtTabletCallTimeAvgFilteredByInstance: vitess_ct.panel.vtgate_to_vttablet_calls {
      title: 'VtGate -> VtTablet Call Time (avg)',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vtgate_vttablet_call_sum[1m]
                )
              )
              /
              sum by (instance)(
                rate(
                  vtgate_vttablet_call_count[1m]
                )
              )
            |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

  },

  singlestats: {
    //      _             _           _        _
    //  ___(_)_ __   __ _| | ___  ___| |_ __ _| |_ ___
    // / __| | '_ \ / _` | |/ _ \/ __| __/ _` | __/ __|
    // \__ \ | | | | (_| | |  __/\__ \ || (_| | |_\__ \
    // |___/_|_| |_|\__, |_|\___||___/\__\__,_|\__|___/
    //              |___/

    vttabletQPS: {
      title: 'QPS - vttablet',
      datasource: '%(dataSource)s' % config._config,
      format: 'short',
      valueFontSize: '70%',
      valueName: 'current',
      sparklineFull: true,
      sparklineShow: true,
      target:
        {
          expr: |||
            sum (
              vitess_mixin:vttablet_query_counts:rate1m
            )
          |||,
          intervalFactor: 1,
        },
    },
  },
}
