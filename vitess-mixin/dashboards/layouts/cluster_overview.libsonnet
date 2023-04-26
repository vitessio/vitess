local helpers = import '../resources/grafonnet/helpers/helpers.libsonnet';
local panels = import '../resources/grafonnet/panels.libsonnet';
local rows = import '../resources/grafonnet/rows.libsonnet';
local singlestats = import '../resources/grafonnet/singlestats.libsonnet';
local templates = import '../resources/grafonnet/templates.libsonnet';
local texts = import '../resources/grafonnet/texts.libsonnet';

local config = import '../../config.libsonnet';

{
  grafanaDashboards+:: {
    'cluster_overview.json':

      helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.clusterOverview)
      .addTemplates([
        templates.interval,
      ])
      .addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
      .addPanels([
        texts.clusterOverview { gridPos: { h: 3, w: 24, x: 0, y: 0 } },
        singlestats.vtgateSuccessRate { gridPos: { h: 4, w: 4, x: 0, y: 3 } },
        singlestats.vttabletQuerySuccess { gridPos: { h: 4, w: 4, x: 4, y: 3 } },
        helpers.vtgate.getSingleStat(config.vtgate.singlestats.vtgateQueryLatencyP99) { gridPos: { h: 4, w: 4, x: 8, y: 3 } },
        helpers.vtgate.getSingleStat(config.vtgate.singlestats.vtgateQPS) { gridPos: { h: 2, w: 4, x: 12, y: 3 } },
        helpers.vttablet.getSingleStat(config.vttablet.singlestats.vttabletQPS) { gridPos: { h: 2, w: 4, x: 12, y: 5 } },
        singlestats.mysqlQPS { gridPos: { h: 2, w: 4, x: 16, y: 3 } },
        singlestats.keyspaceCount { gridPos: { h: 2, w: 2, x: 16, y: 3 } },
        singlestats.shardCount { gridPos: { h: 2, w: 2, x: 18, y: 3 } },
        singlestats.vtgateUp { gridPos: { h: 2, w: 2, x: 20, y: 3 } },
        singlestats.vtctldUp { gridPos: { h: 2, w: 2, x: 20, y: 5 } },
        singlestats.vttabletUp { gridPos: { h: 2, w: 2, x: 22, y: 3 } },

        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequests) { gridPos: { h: 6, w: 8, x: 0, y: 7 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRate) { gridPos: { h: 6, w: 8, x: 8, y: 7 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99) { gridPos: { h: 6, w: 8, x: 16, y: 7 } },

        rows.RED { gridPos: { h: 1, w: 24, x: 0, y: 13 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByKeyspace) { gridPos: { h: 8, w: 8, x: 0, y: 14 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRateByKeyspace) { gridPos: { h: 8, w: 8, x: 8, y: 14 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99ByKeyspace) { gridPos: { h: 8, w: 8, x: 16, y: 14 } },

        rows.tabletsQueries { gridPos: { h: 1, w: 24, x: 0, y: 22 } },
        helpers.vttablet.getPanel(config.vttablet.panels.countServingTablets) { gridPos: { h: 8, w: 8, x: 0, y: 23 } },
        helpers.mysql.getPanel(config.mysql.panels.mysqlSlowQueries) { gridPos: { h: 8, w: 8, x: 8, y: 23 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryTransactionKilled) { gridPos: { h: 8, w: 8, x: 16, y: 23 } },

        rows.serviceRestart { gridPos: { h: 1, w: 24, x: 0, y: 31 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRestart) { gridPos: { h: 8, w: 8, x: 0, y: 32 } },
        panels.vtctldRestart { gridPos: { h: 8, w: 8, x: 8, y: 32 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletRestart) { gridPos: { h: 8, w: 8, x: 16, y: 32 } },

        helpers.mysql.getPanel(config.mysql.panels.mysqlRestart) { gridPos: { h: 8, w: 8, x: 16, y: 40 } },
      ]),

  },
}
