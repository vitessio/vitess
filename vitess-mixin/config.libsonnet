{
  _config+:: {

    // Selectors are inserted between {} in Prometheus queries.
    vtctldSelector: 'job="vitess-vtctld"',
    vtgateSelector: 'job="vitess-vtgate"',
    vttabletSelector: 'job="vitess-vttablet"',
    vtgateNodeSelector: 'job="node-exporter-vitess-vtgate"',
    mysqlSelector: 'job="mysql"',
    defaultTimeFrom: 'now-30m',
    vttabletMountpoint: '/mnt',

    // Datasource to use
    dataSource: 'Prometheus',
    nodeDataSource: 'Prometheus',

    // Default config for the Grafana dashboards in the Vitess Mixin
    grafanaDashboardMetadataDefault: {
      dashboardNameSuffix: '(auto-generated)',
      dashboardAlertPrefix: 'alerts',
      dashboardTags: ['vitess-mixin'],
    },

    dashborardLinks: {
      title: 'vitess-mixin',
      tags: ['vitess-mixin'],
      keepTime: true,
      includeVars: false,
    },

    // Grafana dashboard IDs are necessary for stable links for dashboards
    grafanaDashboardMetadata: {

      local defaultDashboard = {
        environments: ['dev', 'prod'],
        time_from: $._config.defaultTimeFrom,
      },

      // Overview
      clusterOverview+: defaultDashboard {
        uid: 'vitess-cluster-overview',
        title: 'cluster - overview %(dashboardNameSuffix)s' % $._config.grafanaDashboardMetadataDefault,
        description: 'General cluster overview',
        dashboardTags: $._config.grafanaDashboardMetadataDefault.dashboardTags + ['overview', 'cluster'],
      },
      vtgateOverview+: defaultDashboard {
        uid: 'vitess-vtgate-overview',
        title: 'vtgate - overview %(dashboardNameSuffix)s' % $._config.grafanaDashboardMetadataDefault,
        description: 'General vtgate overview',
        dashboardTags: $._config.grafanaDashboardMetadataDefault.dashboardTags + ['overview', 'vtgate'],
      },

      // Host View
      vttabletHostView+: defaultDashboard {
        uid: 'vitess-vttablet-host-view',
        title: 'vttablet - host view %(dashboardNameSuffix)s' % $._config.grafanaDashboardMetadataDefault,
        description: 'Detailed vttablet host view',
        dashboardTags: $._config.grafanaDashboardMetadataDefault.dashboardTags + ['vttablet', 'host'],
      },
      vtgateHostView+: defaultDashboard {
        uid: 'vitess-vtgate-host-view',
        title: 'vtgate - host view %(dashboardNameSuffix)s' % $._config.grafanaDashboardMetadataDefault,
        description: 'Detailed vtgate view by host',
        dashboardTags: $._config.grafanaDashboardMetadataDefault.dashboardTags + ['vtgate', 'host'],
      },
    },
  },

  os: import 'dashboards/resources/config/os_config.libsonnet',
  vttablet: import 'dashboards/resources/config/vttablet_config.libsonnet',
  vtgate: import 'dashboards/resources/config/vtgate_config.libsonnet',
  mysql: import 'dashboards/resources/config/mysql_config.libsonnet',
  row: import 'dashboards/resources/config/row_config.libsonnet',
}
