// Re-cyclable components for panel resources
local config = import '../../../config.libsonnet';
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';

local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;

// TODO: add description for each panel.
// TODO: create a _helper _config file for each group [vtctld],
{
  //        _       _   _     _
  // __   _| |_ ___| |_| | __| |
  // \ \ / / __/ __| __| |/ _` |
  //  \ V /| || (__| |_| | (_| |
  //   \_/  \__\___|\__|_|\__,_|
  //

  //            _
  //  _ __ ___ (_)___  ___
  // | '_ ` _ \| / __|/ __|
  // | | | | | | \__ \ (__
  // |_| |_| |_|_|___/\___|
  //
  local default_notification_config = {
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

  vtctldRestart::
    graphPanel.new(
      'vtctld',
      bars=true,
      datasource='%(dataSource)s' % config._config,
      fill=0,
      format='short',
      legend_values=true,
      legend_alignAsTable=true,
      legend_max=true,
      legend_sort='max',
      legend_sortDesc=false,
      lines=false,
      min=0,
      shared_tooltip=false,
      sort='increasing',
    )
    .addTarget(prometheus.target(
      |||
        sum by (instance) (
          vitess_mixin:process_start_time_seconds_by_instance_job:sum5m{
            %(vtctldSelector)s
          }
        ) > 0
      ||| % config._config,
      legendFormat='{{instance}}'
    )),
}
