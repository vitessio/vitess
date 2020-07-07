local grafana = import 'grafonnet/grafana.libsonnet';
local annotation = grafana.annotation;
local dashboard = grafana.dashboard;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local row = grafana.row;
local singlestat = grafana.singlestat;
local template = grafana.template;

// TODO: add description for each panel
// TODO: extract template to common resource file so that components can be recycled across dashboards

{
  grafanaDashboards+:: {
    'cluster_overview.json':

      // Dashboard metadata
      local dashboardMetadata = $._config.grafanaDashboardMetadata;
      local currentDashboardMetadata = dashboardMetadata.cluster_overview;

      // Datalinks definition
      // TODO: use proper param when grafonnet-lib will support it
      local datalinksDefinition = {
        options: {
          dataLinks: [
            {
              title: 'Go to "Keyspace Overview"',
              url: '/d/%(dashboardID)s/?var-interval=${interval}&var-region=${region}&var-keyspace=${__series.labels.keyspace}&time=${__url_time_range}' % dashboardMetadata.keyspace_overview.uid,
            },
          ],
        },
      };

      // Template definition
      local regionTemplate =
        template.new(
          'region',
          '%(dataSource)s' % $._config,
          'label_values(vtctld_build_number{%(vtctldSelector)s}, region)' % $._config,
          label='Region',
          refresh='time',
          includeAll=false,
          sort=1,
        );

      local intervalTemplate =
        template.new(
          name='interval',
          label='Interval',
          datasource='$datasource',
          query='1m,5m,10m,30m,1h,6h,12h',
          current='5m',
          refresh=2,
          includeAll=false,
          sort=1
        ) + {
          skipUrlSync: false,
          type: 'interval',
          options: [
            {
              selected: false,
              text: '1m',
              value: '1m',
            },
            {
              selected: true,
              text: '5m',
              value: '5m',
            },
            {
              selected: false,
              text: '10m',
              value: '10m',
            },
            {
              selected: false,
              text: '30m',
              value: '30m',
            },
            {
              selected: false,
              text: '1h',
              value: '1h',
            },
            {
              selected: false,
              text: '6h',
              value: '6h',
            },
            {
              selected: false,
              text: '12h',
              value: '12h',
            },
          ],
        };

      // Singlestat definitions
      local querySuccessVTGate =
        singlestat.new(
          'Query success - vtgate',
          datasource='%(dataSource)s' % $._config,
          colorBackground=true,
          decimals=4,
          format='percentunit',
          colors=[
            '#d44a3a',
            'rgba(237, 129, 40, 0.89)',
            '#299c46',
          ],
          valueFontSize='70%',
          valueName='current',
          thresholds='0.99,0.999',
        )
        .addTarget(prometheus.target('1 - sum(rate(vtgate_api_error_counts{%(regionSelector)s, %(vtgateSelector)s}[$interval])) / (sum(rate(vtgate_api_error_counts{%(regionSelector)s, %(vtgateSelector)s}[$interval])) + sum(rate(vtgate_api_count{%(regionSelector)s, %(vtgateSelector)s}[$interval])))\n' % $._config, instant=true, intervalFactor=1));

      local querySuccessVTTablet =
        singlestat.new(
          'Query success - vttablet',
          datasource='%(dataSource)s' % $._config,
          colorBackground=true,
          decimals=4,
          format='percentunit',
          colors=[
            '#d44a3a',
            'rgba(237, 129, 40, 0.89)',
            '#299c46',
          ],
          valueFontSize='70%',
          valueName='current',
          thresholds='0.99,0.999',
        )
        .addTarget(prometheus.target('1 - (sum(rate(vttablet_errors{%(regionSelector)s, %(vttabletSelector)s}[$interval])) / (sum(rate(vttablet_errors{%(regionSelector)s, %(vttabletSelector)s}[$interval])) + sum(rate(vttablet_query_counts{%(regionSelector)s, %(vttabletSelector)s}[$interval]))))' % $._config, instant=true, intervalFactor=1));

      local qpsVTGate =
        singlestat.new(
          'QPS - vtgate',
          datasource='%(dataSource)s' % $._config,
          format='none',
          valueFontSize='70%',
          valueName='current',
          sparklineFull=true,
          sparklineShow=true,
        )
        .addTarget(prometheus.target('sum(rate(vtgate_api_count{%(regionSelector)s, %(vtgateSelector)s}[$interval]))' % $._config, instant=false, intervalFactor=1));

      local qpsVTTablet =
        singlestat.new(
          'QPS - vttablet',
          datasource='%(dataSource)s' % $._config,
          format='none',
          valueFontSize='70%',
          valueName='current',
          sparklineFull=true,
          sparklineShow=true,
        )
        .addTarget(prometheus.target('sum(rate(vttablet_query_counts{%(regionSelector)s, %(vttabletSelector)s}[$interval]))' % $._config, instant=false, intervalFactor=1));

      local qpsMySQL =
        singlestat.new(
          'QPS - MySQL',
          datasource='%(dataSource)s' % $._config,
          format='none',
          valueFontSize='70%',
          valueName='current',
          sparklineFull=true,
          sparklineShow=true,
        )
        .addTarget(prometheus.target('sum(rate(mysql_global_status_queries{%(regionSelector)s, %(mysqlSelector)s}[$interval])) ' % $._config, instant=false, intervalFactor=1));

      local vtctldUp =
        singlestat.new(
          'vtctld',
          datasource='%(dataSource)s' % $._config,
          valueFontSize='50%',
        )
        .addTarget(prometheus.target('sum(up{%(regionSelector)s, %(vtctldSelector)s})' % $._config, instant=true, intervalFactor=1));

      local vtgateUp =
        singlestat.new(
          'vtgate',
          datasource='%(dataSource)s' % $._config,
          valueFontSize='50%',
        )
        .addTarget(prometheus.target('sum(up{%(regionSelector)s, %(vtgateSelector)s})' % $._config, instant=true, intervalFactor=1));

      local vttabletup =
        singlestat.new(
          'vttablet',
          datasource='%(dataSource)s' % $._config,
          valueFontSize='50%',
        )
        .addTarget(prometheus.target('sum(up{%(regionSelector)s, %(vttabletSelector)s})' % $._config, instant=true, intervalFactor=1));

      local vtworkerUp =
        singlestat.new(
          'vtworker',
          datasource='%(dataSource)s' % $._config,
          valueFontSize='50%',
        )
        .addTarget(prometheus.target('sum(up{%(regionSelector)s, %(vtworkerSelector)s})' % $._config, instant=true, intervalFactor=1));

      local keyspaceCount =
        singlestat.new(
          'keyspace',
          datasource='%(dataSource)s' % $._config,
          valueFontSize='50%',
        )
        .addTarget(prometheus.target('count(count(vttablet_tablet_state{%(regionSelector)s, %(vttabletSelector)s}) by (keyspace))' % $._config, instant=true, intervalFactor=1));

      local shardCount =
        singlestat.new(
          'shard',
          datasource='%(dataSource)s' % $._config,
          valueFontSize='50%',
        )
        .addTarget(prometheus.target('count(count(vttablet_tablet_state{%(regionSelector)s, %(vttabletSelector)s}) by (keyspace, shard))' % $._config, instant=true, intervalFactor=1));


      // Panel definitions
      local requests =
        graphPanel.new(
          'Requests',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='rps',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
        )
        .addTarget(prometheus.target('sum(irate(vtgate_queries_processed_by_table{%(regionSelector)s, %(vtgateSelector)s}[$interval]))' % $._config, legendFormat='Requests',));

      local requestsByKeyspace =
        graphPanel.new(
          'Requests (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='rps',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('sum(irate(vtgate_queries_processed_by_table{%(regionSelector)s, %(vtgateSelector)s}[$interval])) by (keyspace)' % $._config, legendFormat='{{keyspace}}',))
        + datalinksDefinition;

      local errorRate =
        graphPanel.new(
          'Error rate',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='percent',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          aliasColors={
            'Error rate': '#F2495C',
          },
        )
        .addTarget(prometheus.target('sum(rate(vtgate_api_error_counts{%(regionSelector)s, %(vtgateSelector)s}[$interval])) /  sum(rate(vtgate_api_count{%(regionSelector)s, %(vtgateSelector)s}[$interval]))' % $._config, legendFormat='Error rate',));

      local errorRateByKeyspace =
        graphPanel.new(
          'Error rate (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='percent',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('sum(rate(vtgate_api_error_counts{%(regionSelector)s, %(vtgateSelector)s}[$interval])) by (keyspace) /  sum(rate(vtgate_api_count{%(regionSelector)s, %(vtgateSelector)s}[$interval])) by (keyspace)' % $._config, legendFormat='{{keyspace}}',))
        + datalinksDefinition;

      local duration =
        graphPanel.new(
          'Duration 99th quantile',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='s',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          aliasColors={
            Duration: '#5794F2',
          },
        )
        .addTarget(prometheus.target('histogram_quantile(0.99, sum(rate(vtgate_api_bucket{%(regionSelector)s, %(vtgateSelector)s}[$interval])) by (le))' % $._config, legendFormat='Duration',));

      local durationByKeyspace =
        graphPanel.new(
          'Duration 99th quantile (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='s',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('histogram_quantile(0.99, sum(rate(vtgate_api_bucket{%(regionSelector)s, %(vtgateSelector)s}[$interval])) by (keyspace, le))' % $._config, legendFormat='{{keyspace}}',))
        + datalinksDefinition;

      local queryPoolAvailableConn =
        graphPanel.new(
          'Query pool: min connections available (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='short',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('min(vttablet_conn_pool_available{%(regionSelector)s, %(vttabletSelector)s}) by (keyspace)' % $._config, legendFormat='{{keyspace}}',));

      local transactionPoolAvailableConn =
        graphPanel.new(
          'Transaction pool: min connections available (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='short',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('min(vttablet_transaction_pool_available{%(regionSelector)s, %(vttabletSelector)s}) by (keyspace)' % $._config, legendFormat='{{keyspace}}',));

      local servingTabletPerShard =
        graphPanel.new(
          'Count of serving tablets (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='short',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('count(vttablet_tablet_server_state{%(regionSelector)s, %(vttabletSelector)s, name="SERVING"}) by (keyspace)' % $._config, legendFormat='{{keyspace}}',));

      local slowQueries =
        graphPanel.new(
          'Slow queries > 0 (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='rps',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
          nullPointMode='null as zero',
        )
        .addTarget(prometheus.target('sum(rate(mysql_global_status_slow_queries{%(regionSelector)s, %(mysqlSelector)s}[$interval])) by (keyspace) > 0' % $._config, legendFormat='{{keyspace}}',));

      local replicationLag =
        graphPanel.new(
          'Replication lag > 0 (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='rps',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('sum(mysql_slave_status_seconds_behind_master{%(regionSelector)s, %(mysqlSelector)s}) by (keyspace) > 0' % $._config, legendFormat='{{keyspace}}',));

      local semiSyncAvgWait =
        graphPanel.new(
          'Semi-sync replication avg wait time (by keyspace)',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='s',
          min=0,
          legend_show=true,
          legend_values=true,
          legend_alignAsTable=true,
          legend_min=true,
          legend_max=true,
          legend_current=true,
          legend_sort='current',
          legend_sortDesc=true,
          sort='decreasing',
          fill=0,
        )
        .addTarget(prometheus.target('sum(rate(mysql_global_status_rpl_semi_sync_master_tx_avg_wait_time{%(regionSelector)s, %(mysqlSelector)s}[$interval])) by (keyspace)' % $._config, legendFormat='{{keyspace}}',));


      // Row definitions
      local topLevel =
        row.new(
          title='Top level'
        );

      local redRow =
        row.new(
          title='RED (Requests / Error rate / Duration)'
        );

      local vttablet =
        row.new(
          title='vttablet'
        );

      local mysql =
        row.new(
          title='MySQL'
        );

      // Dashboard definition
      dashboard.new(
        title=currentDashboardMetadata.title,
        description=currentDashboardMetadata.description,
        uid=currentDashboardMetadata.uid,
        time_from='now-30m',
        tags=(currentDashboardMetadata.dashboardTags),
        editable=true,
        graphTooltip='shared_crosshair',
      )
      .addTemplate(intervalTemplate)
      .addTemplate(regionTemplate)

      // "Top level" row
      .addPanel(topLevel, gridPos={ h: 1, w: 24, x: 0, y: 0 })
      .addPanel(querySuccessVTGate, gridPos={ h: 2, w: 4, x: 0, y: 1 })
      .addPanel(querySuccessVTTablet, gridPos={ h: 2, w: 4, x: 0, y: 3 })
      .addPanel(qpsVTGate, gridPos={ h: 2, w: 4, x: 4, y: 1 })
      .addPanel(qpsVTTablet, gridPos={ h: 2, w: 4, x: 4, y: 3 })
      .addPanel(qpsMySQL, gridPos={ h: 2, w: 4, x: 8, y: 1 })
      .addPanel(keyspaceCount, gridPos={ h: 2, w: 2, x: 8, y: 3 })
      .addPanel(shardCount, gridPos={ h: 2, w: 2, x: 10, y: 3 })
      .addPanel(vtgateUp, gridPos={ h: 2, w: 2, x: 12, y: 1 })
      .addPanel(vttabletup, gridPos={ h: 2, w: 2, x: 14, y: 1 })
      .addPanel(vtctldUp, gridPos={ h: 2, w: 2, x: 12, y: 3 })
      .addPanel(vtworkerUp, gridPos={ h: 2, w: 2, x: 14, y: 3 })

      // RED row
      .addPanel(redRow, gridPos={ h: 1, w: 24, x: 0, y: 5 })
      .addPanel(requests, gridPos={ h: 6, w: 8, x: 0, y: 6 })
      .addPanel(errorRate, gridPos={ h: 6, w: 8, x: 8, y: 6 })
      .addPanel(duration, gridPos={ h: 6, w: 8, x: 16, y: 6 })
      .addPanel(requestsByKeyspace, gridPos={ h: 8, w: 8, x: 0, y: 12 })
      .addPanel(errorRateByKeyspace, gridPos={ h: 8, w: 8, x: 8, y: 12 })
      .addPanel(durationByKeyspace, gridPos={ h: 8, w: 8, x: 16, y: 12 })

      // vttablet
      .addPanel(vttablet, gridPos={ h: 1, w: 24, x: 0, y: 13 })
      .addPanel(queryPoolAvailableConn, gridPos={ h: 6, w: 8, x: 0, y: 14 })
      .addPanel(transactionPoolAvailableConn, gridPos={ h: 6, w: 8, x: 8, y: 14 })
      .addPanel(servingTabletPerShard, gridPos={ h: 6, w: 8, x: 16, y: 14 })

      // MySQL
      .addPanel(mysql, gridPos={ h: 1, w: 24, x: 0, y: 15 })
      .addPanel(slowQueries, gridPos={ h: 6, w: 8, x: 0, y: 16 })
      .addPanel(replicationLag, gridPos={ h: 6, w: 8, x: 8, y: 16 })
      .addPanel(semiSyncAvgWait, gridPos={ h: 6, w: 8, x: 16, y: 16 }),

  },
}
