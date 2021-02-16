local config = import '../../../config.libsonnet';

{
  //Override default_panel values with custom configuration
  prometheus_vitess: {
    panel: {

      datasource: {
        datasource: '%(dataSource)s' % config._config,
      },

      default+: self.datasource {
        fill: 0,
        legend_alignAsTable: true,
        legend_values: true,
        min: 0,
        shared_tooltip: false,
      },

      legend_max+: self.default {
        legend_max: true,
      },

      legend_min_max_avg+: self.default {
        legend_max: true,
        legend_min: true,
        legend_avg: true,
      },

      legend_min_max_current+: self.default {
        legend_max: true,
        legend_min: true,
        legend_current: true,
      },

      null_as_zeros+: self.default {
        nullPointMode: 'null as zero',
      },

      mysql_default+: self.default {
        legend_max: true,
        legend_sort: 'max',
      },

      orc_default+: self.default {
        legend_current: true,
        legend_rightSide: true,
        legend_sort: 'current',
        legend_sortDesc: false,
        pointradius: 5,
        sort: 'none',
      },

      vitess_keyspace_overview+: self.mysql_default {
        fill: 1,
        min: null,
        legend_current: true,
        legend_min: true,
        shared_tooltip: true,
      },

      vitess_vttablet_host_view+: self.mysql_default {
        legend_current: true,
        legend_min: true,
        legend_sort: 'current',
        legend_sortDesc: true,
      },

      go_gc_seconds+: self.legend_max {
        format: 's',
      },

      go_gc_ops+: self.legend_max {
        format: 'ops',
      },

      mysql_timings+: self.legend_min_max_avg {
        legend_sort: 'max',
        legend_sortDesc: true,
        format: 's',
      },

      consolidation_timings+: self.legend_min_max_avg + self.null_as_zeros {
        legend_sort: 'avg',
        legend_sortDesc: true,
      },

      consolidation_timings_seconds: self.consolidation_timings {
        format: 's',
      },

      consolidation_timings_ops: self.consolidation_timings {
        format: 'ops',
      },

      vtgate_to_vttablet_calls: self.legend_min_max_avg + self.null_as_zeros {
        format: 's',
        legend_sortDesc: true,
        legend_sort: 'max',
      },

    },
  },

  prometheus_node: {
    panel: {
      default: {
        datasource: '%(nodeDataSource)s' % config._config,
        fill: 0,
        legend_alignAsTable: true,
        legend_current: true,
        legend_sort: 'current',
        legend_sortDesc: true,
        legend_values: true,
        min: 0,
        sort: 'decreasing',
      },

      percent_panel: self.default {
        format: 'percentunit',
        legend_min: true,
        legend_max: true,
      },

      alerts_tablet: self.percent_panel {
        format: 'percent',
        legend_min: false,
        legend_rightSide: true,
        min: 70,
        max: 100,
      },

      vttablet_host_view: self.percent_panel {
        legend_avg: true,
        legend_current: false,
        legend_sort: 'max',
      },

      keyspace_overview: self.percent_panel {
        decimalsY1: 2,
        legend_avg: true,
        legend_current: false,
        legend_sort: null,
        legend_sortDesc: null,
      },

      performance_analysis_short: self.percent_panel {
        format: 'short',
        legend_min: false,
        legend_max: false,
      },

      performance_analysis_seconds+: self.performance_analysis_short {
        format: 's',
      },
    },
  },
  alerts: {
    // Default values from https://github.com/grafana/grafonnet-lib/blob/2829dc6a4ddd0f7cebe2508bd89bf339dea9a9e6/grafonnet/graph_panel.libsonnet#L247
    default: {
      executionErrorState: 'alerting',
      forDuration: '5m',
      frequency: '60s',
      handler: 1,
      message: '',
      noDataState: 'keep_state',
      alertRuleTags: {},
      prod+: {
        notifications: [],
        conditions: [],
      },
      dev+: self.prod {
      },
    },

    no_data: self.default {
      noDataState: 'no_data',
    },

    orc_default_alert: self.no_data {
      executionErrorState: 'keep_state',
      forDuration: '1m',
      frequency: '1m',
      prod+: {
        notifications: [
          { uid: 'alerts-vitess' },
          { uid: 'pagerduty-vitess' },
        ],
      },
      dev+: {
        notifications: [
          { uid: 'alerts-vitess-dev' },
        ],
      },
    },

    orc_bedrock_alert: self.default {
      noDataState: 'alerting',
      frequency: '1m',
      prod+: {
        notifications: [
          { uid: 'alerts-vitess' },
          { uid: 'pagerduty-vitess' },
        ],
        conditions: [
          {
            evaluatorParams: [1],
            evaluatorType: 'lt',
            operatorType: 'and',
            queryTimeStart: '5m',
            reducerType: 'max',
          },
        ],
      },
      dev+: self.prod {
        notifications: [
          { uid: 'alerts-vitess-dev' },
        ],
      },
    },
  },
}
