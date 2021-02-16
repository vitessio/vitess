// Re-cyclable components for datalinks resources
local config = import '../../config.libsonnet';

// TODO: use proper obj when grafonnet-lib will support it
{
  //TODO move the template to mixin config.
  local dashboard_url_template = '/d/%(dashboardID)s/?var-interval=${interval}&var-region=${region}&var-keyspace=${__series.labels.keyspace}&time=${__url_time_range}',

  keyspaceOverview::
    {
      title: 'Go to "keyspace overview"',
      url: dashboard_url_template
           % config._config.grafanaDashboardMetadata.keyspaceOverview.uid,
    },

  vtgateOverview::
    {
      title: 'Go to "vtgate overview"',
      url: dashboard_url_template
           % config._config.grafanaDashboardMetadata.vtgateOverview.uid,
    },
}
