local heatmaps = import '../resources/grafonnet/heatmaps.libsonnet';
local helpers = import '../resources/grafonnet/helpers/helpers.libsonnet';
local rows = import '../resources/grafonnet/rows.libsonnet';
local templates = import '../resources/grafonnet/templates.libsonnet';
local texts = import '../resources/grafonnet/texts.libsonnet';

local config = import '../../config.libsonnet';
local rows_helper = helpers.default;

{
  grafanaDashboards+:: {
    'vttablet_host_view.json':

      helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.vttabletHostView)
      .addTemplates(
        [
          templates.hostVttablet,
        ]
      ).addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
      .addPanels(
        [
          texts.vttabletHost { gridPos: { h: 3, w: 24, x: 0, y: 0 } },

          rows.RED { gridPos: { h: 1, w: 24, x: 0, y: 4 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 5 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorRateByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 5 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP99ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 5 } },

          rows.REDByPlanType.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByPlanType) { gridPos: { h: 7, w: 8, x: 0, y: 13 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorRateByPlanFilteredByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 13 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP99ByPlan) { gridPos: { h: 7, w: 8, x: 16, y: 13 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 12 } },

          rows.REDByTable.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByTableFilteredByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 29 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorRateByTableFilteredByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 29 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 28 } },

          rows.rowsReturned.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByTableFilteredByInstance) { gridPos: { h: 7, w: 12, x: 0, y: 37 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByPlansFilterByInstance) { gridPos: { h: 7, w: 12, x: 12, y: 37 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 36 } },

          rows_helper.getRow(config.row.queryErrors).addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueriesKilled) { gridPos: { h: 7, w: 8, x: 0, y: 45 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryErrorsByType) { gridPos: { h: 7, w: 8, x: 8, y: 45 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 44 } },

          rows.vitessQueryPool.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolAvailableConnections) { gridPos: { h: 7, w: 8, x: 0, y: 52 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolActiveConnections) { gridPos: { h: 7, w: 8, x: 8, y: 52 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolIddleClosedRate) { gridPos: { h: 7, w: 8, x: 16, y: 52 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolWaitCount) { gridPos: { h: 7, w: 8, x: 0, y: 59 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolAvgWaitTime) { gridPos: { h: 7, w: 8, x: 8, y: 59 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 51 } },

          rows.vitessTransactionPool.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolAvailableConnections) { gridPos: { h: 7, w: 8, x: 0, y: 67 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolActiveConnections) { gridPos: { h: 7, w: 8, x: 8, y: 67 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolIddleClosedRate) { gridPos: { h: 7, w: 8, x: 16, y: 67 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolWaitCount) { gridPos: { h: 7, w: 8, x: 0, y: 74 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolAvgWaitTime) { gridPos: { h: 7, w: 8, x: 8, y: 74 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 66 } },

          rows_helper.getRow(config.row.vitessTimings).addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationAvgByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 82 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP50ByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 82 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP95ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 82 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationAvgByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 89 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationP50ByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 89 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationP95ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 89 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vtgateToVtTabletCallTimeAvgFilteredByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 86 } },
            heatmaps.vttabletQueryTimeDistribution { gridPos: { h: 7, w: 16, x: 8, y: 86 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 81 } },

          rows.mysql.addPanels([
            helpers.mysql.getPanel(config.mysql.panels.mysqlSlowQueriesByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 94 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 93 } },

          rows_helper.getRow(config.row.mysqlTimings).addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletMysqlTimeAvgFilteredByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 102 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletMysqlExecTimeP50FilterebyInstance) { gridPos: { h: 7, w: 8, x: 8, y: 102 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletMysqlExecTimeP95FilterebyInstance) { gridPos: { h: 7, w: 8, x: 16, y: 102 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 101 } },

          rows.OS.addPanels([
            helpers.os.getPanel(config.os.panels.CPUUsageByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 110 } },
            helpers.os.getPanel(config.os.panels.MemoryUsageByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 110 } },
            helpers.os.getPanel(config.os.panels.DiskUsageByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 110 } },
            helpers.os.getPanel(config.os.panels.NetworkTxByInstance) { gridPos: { h: 7, w: 12, x: 0, y: 117 } },
            helpers.os.getPanel(config.os.panels.NetworkRxByInstance) { gridPos: { h: 7, w: 12, x: 12, y: 117 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 109 } },

          rows_helper.getRow(config.row.misc).addPanels([
            helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionCount) { gridPos: { h: 7, w: 8, x: 0, y: 125 } },
            helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionDuration) { gridPos: { h: 7, w: 8, x: 8, y: 125 } },
            helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionDurationQuantiles) { gridPos: { h: 7, w: 8, x: 16, y: 125 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 124 } },
        ],
      ),
  },
}
