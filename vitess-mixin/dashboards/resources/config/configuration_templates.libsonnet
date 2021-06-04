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

      null_as_zeros+: self.default {
        nullPointMode: 'null as zero',
      },

      vttablet_host_view: self.percent_panel {
        legend_avg: true,
        legend_current: false,
        legend_sort: 'max',
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
}
