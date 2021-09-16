local heatmaps = import '../resources/grafonnet/heatmaps.libsonnet';
local helpers = import '../resources/grafonnet/helpers/helpers.libsonnet';
local rows = import '../resources/grafonnet/rows.libsonnet';
local singlestats = import '../resources/grafonnet/singlestats.libsonnet';
local templates = import '../resources/grafonnet/templates.libsonnet';
local texts = import '../resources/grafonnet/texts.libsonnet';

local config = import '../../config.libsonnet';

// TODO: add connections info

{
  grafanaDashboards+:: {
    'vtgate_host_view.json':

      helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.vtgateHostView)
      .addTemplates([
        templates.hostVtgate,
      ])
      .addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
      .addPanels([
        texts.vtgateHost { gridPos: { h: 3, w: 24, x: 0, y: 0 } },
        rows.RED { gridPos: { h: 1, w: 24, x: 0, y: 4 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 5 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRateByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 5 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 5 } },
        rows.REDByTabletType.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByInstanceDBType) { gridPos: { h: 7, w: 8, x: 0, y: 9 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRateByInstanceDBType) { gridPos: { h: 7, w: 8, x: 8, y: 9 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99ByInstanceDBType) { gridPos: { h: 7, w: 8, x: 16, y: 9 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 8 } },
        rows.errors.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByInstanceKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 17 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByInstanceCode) { gridPos: { h: 7, w: 8, x: 8, y: 17 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 16 } },
        rows.duration.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationAVGByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 25 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP50ByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 25 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP95ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 25 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 24 } },
        rows.OS.addPanels([
          helpers.os.getPanel(config.os.panels.CPUUsageByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 33 } },
          helpers.os.getPanel(config.os.panels.MemoryUsageByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 33 } },
          helpers.os.getPanel(config.os.panels.NetworkUsageByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 33 } },
          helpers.os.getPanel(config.os.panels.TCPRetransmissionsByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 40 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 32 } },
      ]),
  },
}
