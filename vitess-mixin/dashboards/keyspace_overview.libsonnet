local grafana = import 'grafonnet/grafana.libsonnet';
local annotation = grafana.annotation;
local dashboard = grafana.dashboard;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local heatmap = grafana.heatmapPanel;
local row = grafana.row;
local singlestat = grafana.singlestat;
local template = grafana.template;

// TODO: add description for each panel
// TODO: extract template to common resource file so that components can be recycled across dashboards

{
  grafanaDashboards+:: {
    'keyspace_overview.json':

      // Dashboard metadata
      local dashboardMetadata = $._config.grafanaDashboardMetadata;
      local currentDashboardMetadata = dashboardMetadata.keyspace_overview;

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

      local keyspaceTemplate =
        template.new(
          'keyspace',
          '%(dataSource)s' % $._config,
          'label_values(vttablet_build_number{%(vttabletSelector)s, region="$region"}, keyspace)' % $._config,
          label='Keyspace',
          refresh='load',
          includeAll=false,
          sort=1,
        );

      local tableTemplate =
        template.new(
          'table',
          '%(dataSource)s' % $._config,
          'label_values(vtgate_queries_processed_by_table{%(vtgateSelector)s, region="$region", keyspace="$keyspace"}, table)' % $._config,
          label='Table',
          refresh='load',
          includeAll=true,
          sort=1,
          allValues='.*',
        );

      // Panel definitions
      local vtgateQpsByTable =
        graphPanel.new(
          'QPS (by table)',
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
        .addTarget(prometheus.target('sum(irate(vtgate_queries_processed_by_table{%(regionSelector)s, %(vtgateSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) by (table)' % $._config, legendFormat='{{table}}',));

      local vtgateQpsByPlanType =
        graphPanel.new(
          'QPS (by plan type)',
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
        .addTarget(prometheus.target('sum(irate(vtgate_queries_processed_by_table{%(regionSelector)s, %(vtgateSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) by (plan)' % $._config, legendFormat='{{plan}}',));

      local vtgateQuerySuccessRate =
        graphPanel.new(
          'Query success rate',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='percent',
          min=0,
          max=100,
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
        .addTarget(prometheus.target('100 - sum(rate(vtgate_api_error_counts{%(regionSelector)s, %(vtgateSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) / (sum(rate(vtgate_api_error_counts{%(regionSelector)s, %(vtgateSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) + sum(rate(vtgate_api_count{%(regionSelector)s, %(vtgateSelector)s, keyspace="$keyspace", table=~"$table"}[$interval]))) * 100' % $._config, legendFormat='Success rate',))
        .addTarget(prometheus.target('sum(rate(vtgate_api_error_counts{%(regionSelector)s, %(vtgateSelector)s, keyspace="$keyspace", table=~"$table"}[$interval]))' % $._config, legendFormat='Error count',));  // TODO: move error count to different Y

      local vttabletQpsByTable =
        graphPanel.new(
          'QPS (by table)',
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
        .addTarget(prometheus.target('sum(irate(vttablet_query_counts{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) by (table)' % $._config, legendFormat='{{table}}',));

      local vttabletQpsByPlanType =
        graphPanel.new(
          'QPS (by plan type)',
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
        .addTarget(prometheus.target('sum(irate(vttablet_query_counts{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) by (plan)' % $._config, legendFormat='{{plan}}',));

      local vttabletQuerySuccessRate =
        graphPanel.new(
          'Query success rate',
          datasource='%(dataSource)s' % $._config,
          span=4,
          format='percent',
          min=0,
          max=100,
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
        .addTarget(prometheus.target('100 - (sum(rate(vttablet_errors{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) / (sum(rate(vttablet_errors{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])) + sum(rate(vttablet_query_counts{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace", table=~"$table"}[$interval])))) * 100' % $._config, legendFormat='Success rate',))
        .addTarget(prometheus.target('sum(rate(vttablet_errors{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace", table=~"$table"}[$interval]))' % $._config, legendFormat='Error count',));  // TODO: move error count to different Y

      local queryTimeAvg =
        graphPanel.new(
          'Query time (avg)',
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
        .addTarget(prometheus.target('sum (rate(vttablet_queries_sum{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) /\nsum (rate(vttablet_queries_count{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval]))' % $._config, legendFormat='avg',));

      local queryTimeP50 =
        graphPanel.new(
          'Query time (p50)',
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
        .addTarget(prometheus.target('histogram_quantile(0.50, sum(rate(vttablet_queries_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le))' % $._config, legendFormat='p50',));

      local queryTimeP95 =
        graphPanel.new(
          'Query time (p95)',
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
        .addTarget(prometheus.target('histogram_quantile(0.95, sum(rate(vttablet_queries_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le))' % $._config, legendFormat='p95',));

      local queryTimeP999 =
        graphPanel.new(
          'Query time (p999)',
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
        .addTarget(prometheus.target('histogram_quantile(0.999, sum(rate(vttablet_queries_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le))' % $._config, legendFormat='p999',));

      local transTimeAvg =
        graphPanel.new(
          'Transaction time (avg)',
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
        .addTarget(prometheus.target('sum (rate(vttablet_transactions_sum{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) /\nsum (rate(vttablet_transactions_count{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval]))' % $._config, legendFormat='avg',));

      local transTimeP50 =
        graphPanel.new(
          'Transaction time (p50)',
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
        .addTarget(prometheus.target('histogram_quantile(0.50, sum(rate(vttablet_transactions_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le))' % $._config, legendFormat='p50',));

      local transTimeP95 =
        graphPanel.new(
          'Transaction time (p95)',
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
        .addTarget(prometheus.target('histogram_quantile(0.95, sum(rate(vttablet_transactions_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le))' % $._config, legendFormat='p95',));

      local transTimeP999 =
        graphPanel.new(
          'Transaction time (p999)',
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
        .addTarget(prometheus.target('histogram_quantile(0.999, sum(rate(vttablet_transactions_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le))' % $._config, legendFormat='p999',));

      // Heatmap definitions
      local queryTimeHeatmap =
        heatmap.new(
          'Query timings heatmap',
          datasource='%(dataSource)s' % $._config,
          dataFormat='tsbuckets',
          yAxis_format='s',
          color_cardColor='#FF9830',
          color_exponent=0.3,
          color_mode='opacity',
          yAxis_decimals=0,
        )
        .addTarget(prometheus.target('sum(rate(vttablet_queries_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le)' % $._config, format='heatmap', legendFormat='{{le}}',));

      local transactionTimeHeatmap =
        heatmap.new(
          'Transaction timings heatmap',
          datasource='%(dataSource)s' % $._config,
          dataFormat='tsbuckets',
          yAxis_format='s',
          color_cardColor='#FF9830',
          color_exponent=0.3,
          color_mode='opacity',
          yAxis_decimals=0,
        )
        .addTarget(prometheus.target('sum(rate(vttablet_queries_bucket{%(regionSelector)s, %(vttabletSelector)s, keyspace="$keyspace"}[$interval])) by (le)' % $._config, format='heatmap', legendFormat='{{le}}',));

      // Row definitions
      local vtgate =
        row.new(
          title='vtgate'
        );

      local vttablet =
        row.new(
          title='vttablet'
        );

      local queryTimings =
        row.new(
          title="Query/Transaction timings (table filter doesn't apply)"  // as we don't have timings by table (yet!)
        );

      // Dashboard definition
      dashboard.new(
        title=currentDashboardMetadata.title,
        description=currentDashboardMetadata.description,
        uid=dashboardMetadata.keyspace_overview.uid,
        time_from='now-30m',
        tags=(currentDashboardMetadata.dashboardTags),
        editable=true,
        graphTooltip='shared_crosshair',
      )
      .addTemplate(intervalTemplate)
      .addTemplate(regionTemplate)
      .addTemplate(keyspaceTemplate)
      .addTemplate(tableTemplate)

      // vtgate row
      .addPanel(vtgate, gridPos={ h: 1, w: 24, x: 0, y: 0 })
      .addPanel(vtgateQpsByTable, gridPos={ h: 7, w: 8, x: 0, y: 1 })
      .addPanel(vtgateQpsByPlanType, gridPos={ h: 7, w: 8, x: 8, y: 1 })
      .addPanel(vtgateQuerySuccessRate, gridPos={ h: 7, w: 8, x: 16, y: 1 })

      // vttablet
      .addPanel(vttablet, gridPos={ h: 1, w: 24, x: 0, y: 8 })
      .addPanel(vttabletQpsByTable, gridPos={ h: 7, w: 8, x: 0, y: 9 })
      .addPanel(vttabletQpsByPlanType, gridPos={ h: 7, w: 8, x: 8, y: 9 })
      .addPanel(vttabletQuerySuccessRate, gridPos={ h: 7, w: 8, x: 16, y: 9 })

      // queryTimings row
      .addPanel(queryTimings, gridPos={ h: 1, w: 24, x: 0, y: 15 })
      .addPanel(queryTimeAvg, gridPos={ h: 6, w: 6, x: 0, y: 16 })
      .addPanel(queryTimeP50, gridPos={ h: 6, w: 6, x: 6, y: 16 })
      .addPanel(queryTimeP95, gridPos={ h: 6, w: 6, x: 12, y: 16 })
      .addPanel(queryTimeP999, gridPos={ h: 6, w: 6, x: 18, y: 16 })
      .addPanel(transTimeAvg, gridPos={ h: 6, w: 6, x: 0, y: 23 })
      .addPanel(transTimeP50, gridPos={ h: 6, w: 6, x: 6, y: 23 })
      .addPanel(transTimeP95, gridPos={ h: 6, w: 6, x: 12, y: 23 })
      .addPanel(transTimeP999, gridPos={ h: 6, w: 6, x: 18, y: 23 })
      .addPanel(queryTimeHeatmap, gridPos={ h: 8, w: 12, x: 0, y: 29 })
      .addPanel(transactionTimeHeatmap, gridPos={ h: 8, w: 12, x: 12, y: 29 }),

  },
}
