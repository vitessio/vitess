local heatmaps = import '../resources/grafonnet/heatmaps.libsonnet';
local helpers = import '../resources/grafonnet/helpers/helpers.libsonnet';
local rows = import '../resources/grafonnet/rows.libsonnet';
local templates = import '../resources/grafonnet/templates.libsonnet';
local texts = import '../resources/grafonnet/texts.libsonnet';

local config = import '../../config.libsonnet';

// TODO: add OLAP mode
{
  grafanaDashboards+:: {
    'keyspace_overview.json':

      helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.keyspaceOverview)
      .addTemplates(
        [
          templates.interval,
          templates.region,
          templates.keyspace,
          templates.table,
        ]
      ).addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
      .addPanels(
        [
          texts.keyspaceOverview { gridPos: { h: 3, w: 24, x: 0, y: 0 } },

          rows.RED { gridPos: { h: 1, w: 24, x: 0, y: 3 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 4 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorRateByKeyspace) { gridPos: { h: 7, w: 8, x: 8, y: 4 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP99ByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 4 } },

          rows.vtgate.addPanels([
            helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByTable) { gridPos: { h: 7, w: 8, x: 0, y: 12 } },
            helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByPlanType) { gridPos: { h: 7, w: 8, x: 8, y: 12 } },
            helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsSuccessRateFilterByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 12 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 11 } },

          rows.vttablet.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByTable) { gridPos: { h: 7, w: 8, x: 0, y: 20 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByPlanType) { gridPos: { h: 7, w: 8, x: 8, y: 20 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsSuccessRateFilterByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 20 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 19 } },

          rows.errors.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorByTablesFilterByKeyspaceTables) { gridPos: { h: 7, w: 8, x: 0, y: 28 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorByPlanFilterByKeyspaceTables) { gridPos: { h: 7, w: 8, x: 8, y: 28 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorByShardFilterByKeyspaceTables) { gridPos: { h: 7, w: 8, x: 16, y: 28 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 27 } },

          rows.rowsReturned.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByTablesFilterByKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 35 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByPlansFilterByKeyspace) { gridPos: { h: 7, w: 8, x: 8, y: 35 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByShardFilterByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 35 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 34 } },

          rows.queryTimings.addPanels([
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationAvg) { gridPos: { h: 6, w: 6, x: 0, y: 43 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP50) { gridPos: { h: 6, w: 6, x: 6, y: 43 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP95) { gridPos: { h: 6, w: 6, x: 12, y: 43 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP999) { gridPos: { h: 6, w: 6, x: 18, y: 43 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransDurationAvg) { gridPos: { h: 6, w: 6, x: 0, y: 49 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransDurationP50) { gridPos: { h: 6, w: 6, x: 6, y: 49 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransDurationP95) { gridPos: { h: 6, w: 6, x: 12, y: 49 } },
            helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransDurationP999) { gridPos: { h: 6, w: 6, x: 18, y: 49 } },
            heatmaps.vttabletQueryDuration { gridPos: { h: 8, w: 12, x: 0, y: 55 } },
            heatmaps.vttabletTransactionDuration { gridPos: { h: 8, w: 12, x: 12, y: 55 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 42 } },

          rows.mysql.addPanels([
            helpers.mysql.getPanel(config.mysql.panels.mysqlSlowQueriesByKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 63 } },
            helpers.mysql.getPanel(config.mysql.panels.mysqlReplicationLagByKeyspace) { gridPos: { h: 7, w: 8, x: 8, y: 63 } },
            helpers.mysql.getPanel(config.mysql.panels.mysqlInnoDBRowsReadOperationsByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 63 } },
            helpers.mysql.getPanel(config.mysql.panels.mysqlSemiSyncAvgWaitByKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 70 } },
            helpers.mysql.getPanel(config.mysql.panels.mysqlVersionByKeyspace) { gridPos: { h: 7, w: 8, x: 8, y: 70 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 62 } },

          rows.OS.addPanels([
            helpers.os.getPanel(config.os.panels.CPUUsageByKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 78 } },
            helpers.os.getPanel(config.os.panels.MemoryUsageByKeyspace) { gridPos: { h: 7, w: 8, x: 8, y: 78 } },
            helpers.os.getPanel(config.os.panels.NetworkUsageByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 78 } },
            helpers.os.getPanel(config.os.panels.DiskUsageByKeyspace) { gridPos: { h: 7, w: 8, x: 0, y: 85 } },
          ]) { gridPos: { h: 1, w: 24, x: 0, y: 77 } },
        ],
      ),
  },
}
