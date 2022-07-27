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
    'vtgate_overview.json':

      helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.vtgateOverview)
      .addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
      .addPanels([
        texts.vtgateOverview { gridPos: { h: 3, w: 24, x: 0, y: 0 } },

        rows.RED { gridPos: { h: 1, w: 24, x: 0, y: 4 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequests) { gridPos: { h: 7, w: 8, x: 0, y: 5 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRate) { gridPos: { h: 7, w: 8, x: 8, y: 5 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99) { gridPos: { h: 7, w: 8, x: 16, y: 5 } },

        rows.REDByKeyspace.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 13 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRateByKeyspace) { gridPos: { h: 7, w: 8, x: 8, y: 13 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99ByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 13 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 12 } },

        rows.REDByTabletType.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByDBType) { gridPos: { h: 7, w: 8, x: 0, y: 21 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRateByDBType) { gridPos: { h: 7, w: 8, x: 8, y: 21 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99ByDBType) { gridPos: { h: 7, w: 8, x: 16, y: 21 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 20 } },

        rows.errors.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByCode) { gridPos: { h: 7, w: 8, x: 0, y: 29 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByOperation) { gridPos: { h: 7, w: 8, x: 8, y: 29 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByDbtype) { gridPos: { h: 7, w: 8, x: 16, y: 29 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 28 } },

        rows.duration.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationAVG) { gridPos: { h: 7, w: 8, x: 0, y: 37 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP50) { gridPos: { h: 7, w: 8, x: 8, y: 37 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP95) { gridPos: { h: 7, w: 8, x: 16, y: 37 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 36 } },

        rows.OS.addPanels([
          helpers.os.getPanel(config.os.panels.CPUUsage) { gridPos: { h: 7, w: 8, x: 0, y: 45 } },
          helpers.os.getPanel(config.os.panels.MemoryUsage) { gridPos: { h: 7, w: 8, x: 8, y: 45 } },
          helpers.os.getPanel(config.os.panels.NetworkUsage) { gridPos: { h: 7, w: 8, x: 16, y: 45 } },
          helpers.os.getPanel(config.os.panels.TCPRetransmissions) { gridPos: { h: 7, w: 8, x: 16, y: 52 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 44 } },
      ]),
  },
}
