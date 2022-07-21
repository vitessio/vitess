/** This is a configuration file containing metadata for vtgate grafana resources. */

local config = import '../../../config.libsonnet';
local configuration_templates = import './configuration_templates.libsonnet';
local vitess_ct = configuration_templates.prometheus_vitess;

// TODO: move local template variables and fields to ./configuration_templates.libsonnet.
{
  //  ____                  _
  // |  _ \ __ _ _ __   ___| |___
  // | |_) / _` | '_ \ / _ \ / __|
  // |  __/ (_| | | | |  __/ \__ \
  // |_|   \__,_|_| |_|\___|_|___/

  // TODO: add description for each panel.
  panels: {

    //Override default_panel values with custom configuration
    local panel_template = {
      datasource: '%(dataSource)s' % config._config,
      format: 'rps',
      fill: 0,
      legend_values: true,
      legend_alignAsTable: true,
      legend_min: true,
      legend_max: true,
      legend_current: true,
      legend_sort: 'current',
      legend_sortDesc: true,
      min: 0,
      sort: 'decreasing',
    },

    local garbage_collector_panel_template = panel_template {
      format: 's',
      legend_sort: 'max',
    },

    vtgateRequests:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests',
        fill: 1,
        targets: [
          {
            expr: |||
              sum (
                vitess_mixin:vtgate_api_count:rate1m
              )
            |||,
            legendFormat: 'Requests',
          },
        ],
      },

    vtgateRequestsByKeyspace:
      panel_template +
      vitess_ct.panel.null_as_zeros {
        title: 'Requests (by keyspace)',
        targets: [
          {
            expr: |||
              sum by(keyspace)(
                vitess_mixin:vtgate_api_count_by_keyspace:rate1m
              )
            ||| % config._config,
            legendFormat: '{{keyspace}}',
          },
        ],
      },


    vtgateRequestsByDBType:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests (by db_type)',
        targets: [
          {
            expr: |||
              sum by (db_type)(
                vitess_mixin:vtgate_api_count_by_db_type:rate1m
              )
            |||,
            legendFormat: '{{db_type}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vtgateRequestsByInstanceDBType:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests (by db_type)',
        fill: 0,
        targets: [
          {
            expr: |||
              sum by (instance, db_type)(
                rate(
                  vtgate_api_count{
                    instance=~"$host",
                  }[1m]
                )
              )
            |||,
            legendFormat: '{{instance}} - {{db_type}}',
            intervalFactor: 1,
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS TARGET
    vtgateRequestsByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Requests',
        fill: 0,
        targets: [
          {
            expr:
              |||
                sum by (instance)(
                  rate(
                    vtgate_api_count{
                      instance=~'$host'
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}}',
            intervalFactor: 1,
          },
        ],
      },

    vtgateErrorRate:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate',
        format: 'percentunit',
        fill: 1,
        aliasColors: {
          'Error rate': '#F2495C',
        },
        targets: [
          {
            expr: |||
              sum (
                vitess_mixin:vtgate_api_error_counts:rate1m)
              /
              sum (
                vitess_mixin:vtgate_api_count:rate1m)
            |||,
            legendFormat: 'Error rate',
          },
        ],
      },

    vtgateErrorRateByKeyspace:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate (by keyspace)',
        format: 'percentunit',
        targets: [
          {
            expr: |||
              sum by(keyspace)(
                vitess_mixin:vtgate_api_error_counts_by_keyspace:rate1m)
              /
              sum by(keyspace)(
                vitess_mixin:vtgate_api_count_by_keyspace:rate1m)
            |||,
            legendFormat: '{{keyspace}}',
          },
        ],
      },

    vtgateErrorRateByDBType:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate (by db_type)',
        format: 'percentunit',
        targets: [
          {
            expr: |||
              sum by (db_type)(
                vitess_mixin:vtgate_api_error_counts_by_db_type:rate1m
              )
              /
              sum by (db_type)(
                vitess_mixin:vtgate_api_count_by_db_type:rate1m
              )
            ||| % config._config,
            legendFormat: '{{db_type}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS TARGET
    vtgateErrorRateByInstance:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate',
        fill: 0,
        format: 'percentunit',
        nullPointMode: 'null as zero',
        targets: [
          {
            expr:
              |||
                sum by(instance)(
                  rate(
                    vtgate_api_error_counts[1m]
                    ) > 0
                )
                /
                sum by(instance)(
                  rate(
                    vtgate_api_count[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}}',
            intervalFactor: 1,
          },
        ],
      },

    //TODO Create RECORDING RULES FOR THESE PROM TARGETS
    vtgateErrorRateByInstanceDBType:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Error rate (by db_type)',
        fill: 0,
        format: 'percentunit',
        targets: [
          {
            expr:
              |||
                sum by(instance, db_type)(
                  rate(vtgate_api_error_counts{
                      instance=~"$host"
                    }[1m]
                  ) > 0
                )
                /
                sum by(instance, db_type)(
                  rate(
                    vtgate_api_count{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}} - {{db_type}}',
            intervalFactor: 1,
          },
        ],
      },

    vtgateDurationP99:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Duration 99th quantile',
        fill: 1,
        format: 's',
        aliasColors: {
          Duration: '#5794F2',
        },
        targets: [
          {
            expr: |||
              histogram_quantile(
                0.99,
                sum by(le)(
                  vitess_mixin:vtgate_api_bucket:rate1m
                )
              )
            |||,
            legendFormat: 'Duration',
          },
        ],
      },

    vtgateDurationP99ByKeyspace:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Duration 99th quantile (by keyspace)',
        format: 's',
        targets: [
          {
            expr: |||
              histogram_quantile(
                0.99,
                sum by(keyspace,le)(
                  vitess_mixin:vtgate_api_bucket_by_keyspace:rate1m
                )
              )
            ||| % config._config,
            legendFormat: '{{keyspace}}',
          },
        ],
      },

    local vtgateDurationTemplate =
      panel_template
      + vitess_ct.panel.null_as_zeros {
        fill: 1,
        format: 's',
        aliasColors: {
          Duration: '#5794F2',
        },
      },

    //TODO crete a recording rule for this prometheus vitess target
    vtgateDurationP99ByInstance: vtgateDurationTemplate {
      title: 'Duration 99th quantile',
      fill: 0,
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.99,
              sum by(instance,le)(
                rate(
                  vtgate_api_bucket{
                    instance=~"$host"
                  }[1m]
                )
              )
            )
          |||,
          legendFormat: '{{instance}}',
          intervalFactor: 1,
        },
      ],
    },

    //TODO crete a recording rule for this prometheus vitess target
    vtgateDurationP99ByInstanceDBType: vtgateDurationTemplate {
      title: 'Duration 99th quantile (by db_type)',
      fill: 0,
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.99,
              sum by(instance,db_type,le)(
                rate(
                  vtgate_api_bucket{
                    instance=~"$host"
                  }[1m]
                )
              )
            )
          |||,
          legendFormat: '{{instance}} - {{db_type}}',
          intervalFactor: 1,
        },
      ],
    },

    vtgateDurationP50: vtgateDurationTemplate {
      title: 'Duration 50th quantile',
      fill: 0,
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.50,
              sum by(le)(
                vitess_mixin:vtgate_api_bucket:rate1m
              )
            )
          |||,
          legendFormat: 'Duration p50',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vtgateDurationP50ByInstance: vtgateDurationTemplate {
      title: 'Duration 50th quantile',
      fill: 0,
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.50,
              sum by(instance, le)(
                rate(
                  vtgate_api_bucket{
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

    vtgateDurationP95: vtgateDurationTemplate {
      title: 'Duration 95th quantile',
      fill: 0,
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.95,
              sum by(le)(
                vitess_mixin:vtgate_api_bucket:rate1m
              )
            )
          |||,
          legendFormat: 'Duration p95',
        },
      ],
    },

    vtgateDurationP95ByInstance: vtgateDurationTemplate {
      title: 'Duration 95th quantile',
      fill: 0,
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.95,
              sum by(instance, le)(
                rate(
                  vtgate_api_bucket{
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

    //TODO write a recording rule for this prometheus_vitess target
    // only vtgate_api_sum requires a rule. Use 1m interval instead of 5m.
    vtgateDurationAVG: vtgateDurationTemplate {
      title: 'Duration (Avg)',
      fill: 0,
      targets: [
        {
          expr: |||
            sum (
              rate(
                vtgate_api_sum[5m]
              )
            )
            /
            sum (
              rate(
                vtgate_api_count[5m]
                )
              )
          |||,
          legendFormat: 'Avg Latency',
        },
      ],
    },

    //TODO write a recording rule for this prometheus_vitess target
    vtgateDurationAVGByInstance: vtgateDurationTemplate {
      title: 'Duration (Avg)',
      fill: 0,
      targets: [
        {
          expr: |||
            sum by (instance)(
              rate(
                vtgate_api_sum{
                  instance=~"$host"
                }[5m]
              )
            )
            /
            sum by (instance)(
              rate(
                vtgate_api_count{
                  instance=~"$host"
                  }[5m]
                )
              )
          |||,
          legendFormat: '{{instance}}',
        },
      ],
    },

    vtgateDurationP99ByDBType:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Duration 99th quantile (by db_type)',
        format: 's',
        targets: [
          {
            expr: |||
              histogram_quantile(
                0.99,
                sum by (db_type, le)(
                  vitess_mixin:vtgate_api_bucket_by_db_type:rate1m
                )
              )
            |||,
            legendFormat: '{{db_type}}',
          },
        ],
      },

    vtgateErrorsByCode:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Errors (by code)',
        format: 'cps',
        targets: [
          {
            expr: |||
              sum by (code)(
                vitess_mixin:vtgate_api_error_counts_by_code:rate1m
              )
            |||,
            legendFormat: '{{code}}',
          },
        ],

      },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vtgateErrorsByInstanceCode:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Errors (by code)',
        format: 'cps',
        targets: [
          {
            expr: |||
              sum by (instance,code)(
                rate(
                  vtgate_api_error_counts{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
            legendFormat: '{{instance}} - {{code}}',
          },
        ],

      },

    vtgateErrorsByOperation:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Errors (by operation)',
        format: 'cps',
        targets: [
          {
            expr: |||
              sum by (operation)(
                vitess_mixin:vtgate_api_error_counts_by_operation:rate1m
              )
            |||,
            legendFormat: '{{operation}}',
          },
        ],

      },

    vtgateErrorsByDbtype:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Errors (by db_type)',
        format: 'cps',
        targets: [
          {
            expr: |||
              sum by (db_type)(
                vitess_mixin:vtgate_api_error_counts_by_db_type:rate1m
              )
            |||,
            legendFormat: '{{db_type}}',
          },
        ],

      },

    //TODO CREATE RECORDING RULE FOR THIS PROM TARGET
    vtgateErrorsByInstanceKeyspace:
      panel_template
      + vitess_ct.panel.null_as_zeros {
        title: 'Errors (by keyspace)',
        format: 'cps',
        targets: [
          {
            expr: |||
              sum by (instance,keyspace)(
                rate(
                  vtgate_api_error_counts{
                    instance=~"$host"
                  }[1m]
                )
              )
            |||,
            legendFormat: '{{keyspace}}',
            intervalFactor: 1,
          },
        ],
      },

    vtgateRestart: {
      title: 'vtgate',
      bars: true,
      datasource: '%(dataSource)s' % config._config,
      fill: 0,
      format: 'short',
      legend_alignAsTable: true,
      legend_current: false,
      legend_max: true,
      legend_min: false,
      legend_sort: 'max',
      legend_sortDesc: false,
      legend_values: true,
      lines: false,
      min: 0,
      shared_tooltip: false,
      sort: 'increasing',
      targets: [
        {
          expr: |||
            sum by (instance)(
              vitess_mixin:process_start_time_seconds_by_instance_job:sum5m{
                %(vtgateSelector)s
              }
            )
          ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO crete a recording rule for this prometheus vitess target
    vtgateGarbageCollectionCount: garbage_collector_panel_template {
      title: 'GC Count',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  go_gc_duration_seconds_count{
                    %(vtgateSelector)s
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
          intervalFactor: 1,
        },
      ],
    },
    //TODO crete a recording rule for this prometheus vitess target
    vtgateGarbageCollectionDuration: garbage_collector_panel_template {
      title: 'GC Duration total per second',
      description: 'A summary of the pause duration of garbage collection cycles',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  go_gc_duration_seconds_count{
                    %(vtgateSelector)s
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
          intervalFactor: 1,
        },
      ],
    },
    //TODO crete a recording rule for this prometheus vitess target
    vtgateGarbageCollectionDurationQuantiles: garbage_collector_panel_template {
      title: 'GC Duration quantiles',
      targets: [
        {
          expr:
            |||
              sum by(quantile)(
                rate(
                  go_gc_duration_seconds{
                    %(vtgateSelector)s
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: 'p{{quantile}}',
          intervalFactor: 1,
        },
      ],
    },
  },

  //      _             _           _        _
  //  ___(_)_ __   __ _| | ___  ___| |_ __ _| |_ ___
  // / __| | '_ \ / _` | |/ _ \/ __| __/ _` | __/ __|
  // \__ \ | | | | (_| | |  __/\__ \ || (_| | |_\__ \
  // |___/_|_| |_|\__, |_|\___||___/\__\__,_|\__|___/
  //              |___/

  //TODO move default configurations to helper code (vttablet_helper)
  singlestats: {

    vtgateQPS: {
      title: 'QPS - vtgate',
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
              vitess_mixin:vtgate_api_count:rate1m
            )
          |||,
          intervalFactor: 1,
        },
    },

    vtgateQueryLatencyP99: {
      title: 'Query latency p99',
      datasource: '%(dataSource)s' % config._config,
      colorBackground: true,
      decimals: 2,
      format: 'ms',
      valueFontSize: '70%',
      valueName: 'current',
      thresholds: '30,50',
      target:
        {
          expr: |||
            1000 * histogram_quantile(
              0.99,
              sum by(le)(
                vitess_mixin:vtgate_api_bucket:rate1m
              )
            )
          |||,
          instant: true,
          intervalFactor: 1,
        },
    },
  },
}
