{
  _config+:: {

    // Selectors are inserted between {} in Prometheus queries.
    regionSelector: 'region="$region"',
    vtctldSelector: 'job="vitess-vtctld"',
    vtgateSelector: 'job="vitess-vtgate"',
    vttabletSelector: 'job="vitess-vttablet"',
    vtworkerSelector: 'job="vitess-vtworker"',
    mysqlSelector: 'job="mysql"',

    // Datasource to use
    dataSource: 'Prometheus_Vitess',

    // Default config for the Grafana dashboards in the Vitess Mixin
    grafanaDashboardMetadataDefault: {
      dashboardNamePrefix: 'Vitess /',
      dashboardNameSuffix: '(auto-generated)',
      dashboardTags: ['vitess-mixin'],
    },

    // Grafana dashboard IDs are necessary for stable links for dashboards
    grafanaDashboardMetadata: {
      cluster_overview: {
        uid: '0d0778047f5a64ff2ea084ec3e',
        title: '%(dashboardNamePrefix)s Cluster Overview %(dashboardNameSuffix)s' % $._config.grafanaDashboardMetadataDefault,
        description: 'Vitess cluster overview',
        dashboardTags: $._config.grafanaDashboardMetadataDefault.dashboardTags + ['overview', 'cluster'],
      },
      keyspace_overview: {
        uid: 'ff33eceed7d2b1267dd286a099',
        title: '%(dashboardNamePrefix)s Keyspace Overview %(dashboardNameSuffix)s' % $._config.grafanaDashboardMetadataDefault,
        description: 'General keyspace overview',
        dashboardTags: $._config.grafanaDashboardMetadataDefault.dashboardTags + ['overview', 'keyspace'],
      },
    },

  },
}
