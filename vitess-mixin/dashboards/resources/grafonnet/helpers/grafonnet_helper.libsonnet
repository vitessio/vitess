/**
 * This is a helper library to generate grafana resources reading the configuration from a config file.
 */

local grafana = import '../../../../vendor/grafonnet/grafana.libsonnet';
local prometheus = grafana.prometheus;
local alert_condition = grafana.alertCondition;

//                         _        _   _
//   __ _ _ __  _ __   ___ | |_ __ _| |_(_) ___  _ __
//  / _` | '_ \| '_ \ / _ \| __/ _` | __| |/ _ \| '_ \
// | (_| | | | | | | | (_) | || (_| | |_| | (_) | | | |
//  \__,_|_| |_|_| |_|\___/ \__\__,_|\__|_|\___/|_| |_|

local getAnnotation(config) = if (config.name == 'default') then
  grafana.annotation.default
else
  // TODO when the properties are supported by grafonnet use the lib constructor
  // instead of using  composition
  grafana.annotation.datasource(
    config.name,
    config.datasource
  ) +
  config.properties;

//      _           _     _                         _
//   __| | __ _ ___| |__ | |__   ___   __ _ _ __ __| |
//  / _` |/ _` / __| '_ \| '_ \ / _ \ / _` | '__/ _` |
// | (_| | (_| \__ \ | | | |_) | (_) | (_| | | | (_| |
//  \__,_|\__,_|___/_| |_|_.__/ \___/ \__,_|_|  \__,_|

local getDashboard(config) = grafana.dashboard.new(
  title=config.title,
  description=config.description,
  uid=config.uid,
  time_from=config.time_from,
  tags=(config.dashboardTags),
  editable=true,
  graphTooltip='shared_crosshair',
) + {
  environments:: config.environments,
};

//                         _
//  _ __   __ _ _ __   ___| |
// | '_ \ / _` | '_ \ / _ \ |
// | |_) | (_| | | | |  __/ |
// | .__/ \__,_|_| |_|\___|_|
// |_|
// The default panel contains all the parameters that we want to override.
// https://github.com/grafana/grafonnet-lib/blob/master/grafonnet/graph_panel.libsonnet

local default_panel = {
  title: '',
  aliasColors: {},
  bars: false,
  decimals: null,
  description: null,
  format: 'short',
  fill: 1,
  legend_alignAsTable: false,
  legend_avg: false,
  legend_current: false,
  legend_hideZero: null,
  legend_max: false,
  legend_min: false,
  legend_rightSide: false,
  legend_sort: null,
  legend_sortDesc: null,
  legend_values: false,
  lines: true,
  linewidth: 1,
  max: null,
  min: null,
  points: false,
  pointradius: 5,
  nullPointMode: 'null',
  shared_tooltip: true,
  sort: 0,
  thresholds: [],
};

local default_prometheus_target = {
  format: 'time_series',
  instant: null,
  intervalFactor: 2,
  legendFormat: '',
};

local default_alert_condition = {
  evaluatorParams: [],
  evaluatorType: 'gt',
  operatorType: 'and',
  queryRefId: 'A',
  queryTimeEnd: 'now',
  queryTimeStart: '5m',
  reducerParams: [],
  reducerType: 'avg',
};

local getConditions(config) =
  if std.objectHas(config.alert[std.extVar('env')], 'conditions') then
    //reducerType is a grafonnet field value. This asserts the config is not legacy
    if std.objectHas(config.alert[std.extVar('env')].conditions[0], 'reducerType')
    then
      local x = std.map(
        function(c) default_alert_condition + c
        , config.alert[std.extVar('env')].conditions
      );
      std.map(
        function(c)
          alert_condition.new(
            evaluatorParams=c.evaluatorParams,
            evaluatorType=c.evaluatorType,
            operatorType=c.operatorType,
            queryRefId=c.queryRefId,
            queryTimeEnd=c.queryTimeEnd,
            queryTimeStart=c.queryTimeStart,
            reducerParams=c.reducerParams,
            reducerType=c.reducerType,
          )
        , x
      )
    else
      //Legacy config files include calls to grafonnet.alert_condition.new()
      //TODO update legacy config files to use alert conditions in json format,
      config.alert[std.extVar('env')].conditions
  else [];

local getTargets(config) =
  if std.objectHas(config, 'targets') then
    if config.datasource != null &&
       std.startsWith(config.datasource, 'Prometheus') &&
       std.objectHas(config.targets[0], 'expr')
    then
      local x = std.map(
        function(t) default_prometheus_target + t
        , config.targets
      );
      std.map(
        function(t)
          prometheus.target(
            t.expr,
            legendFormat=t.legendFormat,
            instant=t.instant,
            intervalFactor=t.intervalFactor,
            format=t.format
          )
        , x
      )
    else
      //When the datasource is not prometheus(elastic, graphite) config file
      //include calls to graphite.target() and elasticsearch.target().
      //see webapp_config.lisonnet
      //TODO Update this method to decouple grafonnet code from the configuration files.
      //Legacy configuration files include prometheus.target() calls.
      //TODO update legacy config files to use {'expr':'Prom query' ...} format,
      config.targets
  else [];

// This method overriddes grafonnet graphPanel defaults with the values in the config file .
// https://github.com/grafana/grafonnet-lib/blob/master/grafonnet/graph_panel.libsonnet
// TODO: When grapPanel supports either addLinks (https://github.com/grafana/grafonnet-lib/pull/278)
// we should add the links there instead of composing the `options` field.
local initPanel(config) =
  grafana.graphPanel.new(
    title=config.title,
    aliasColors=config.aliasColors,
    bars=config.bars,
    datasource=config.datasource,
    decimals=config.decimals,
    description=config.description,
    fill=config.fill,
    format=config.format,
    legend_alignAsTable=config.legend_alignAsTable,
    legend_avg=config.legend_avg,
    legend_rightSide=config.legend_rightSide,
    legend_hideZero=config.legend_hideZero,
    legend_min=config.legend_min,
    legend_max=config.legend_max,
    legend_current=config.legend_current,
    legend_sort=config.legend_sort,
    legend_sortDesc=config.legend_sortDesc,
    legend_values=config.legend_values,
    lines=config.lines,
    linewidth=config.linewidth,
    max=config.max,
    min=config.min,
    points=config.points,
    pointradius=config.pointradius,
    nullPointMode=config.nullPointMode,
    shared_tooltip=config.shared_tooltip,
    sort=config.sort,
    thresholds=config.thresholds,
  ).addTargets(
    getTargets(config)
  ) +
  {
    [if std.objectHas(config, 'options')
    then 'options']:
      config.options,
  };

local getPanel(c) =
  if std.objectHas(c, 'alert') then
    local config = default_panel + c;
    local panel = initPanel(config).addAlert(
      config.alert.name,
      executionErrorState=config.alert.executionErrorState,
      forDuration=config.alert.forDuration,
      frequency=config.alert.frequency,
      message=config.alert.message,
      noDataState=config.alert.noDataState,
      notifications=config.alert[std.extVar('env')].notifications,
    ).addConditions(
      getConditions(config)
    );
    if std.objectHas(config, 'seriesOverrides') then
      local it = panel;
      std.foldl(function(p, o) p.addSeriesOverride(o), config.seriesOverrides, it)
    else
      panel
  else
    (local config = default_panel + c;
     local panel = initPanel(config);
     if std.objectHas(config, 'seriesOverrides') then
       local it = panel;
       std.foldl(function(p, o) p.addSeriesOverride(o), config.seriesOverrides, it)
     else
       panel);

//  _ __ _____      __
// | '__/ _ \ \ /\ / /
// | | | (_) \ V  V /
// |_|  \___/ \_/\_/

local row_default = {
  title: '',
  height: null,
  collapse: false,
  repeat: null,
  showTitle: null,
  titleSize: 'h6',
};

local getRow(c) =
  local config = row_default + c;
  grafana.row.new(
    title=config.title,
    height=config.height,
    collapse=config.collapse,
    repeat=config.repeat,
    showTitle=config.showTitle,
    titleSize=config.titleSize
  );

//      _             _           _        _
//  ___(_)_ __   __ _| | ___  ___| |_ __ _| |_
// / __| | '_ \ / _` | |/ _ \/ __| __/ _` | __|
// \__ \ | | | | (_| | |  __/\__ \ || (_| | |_
// |___/_|_| |_|\__, |_|\___||___/\__\__,_|\__|
//              |___/
//The default value should include all the parameters that are overridden by the objects that extend the default.
//Default values match grafonnet defaults > https://github.com/grafana/grafonnet-lib/blob/master/grafonnet/singlestat.libsonnet

local default_singlestat = {
  colors: [
    '#299c46',
    'rgba(237, 129, 40, 0.89)',
    '#d44a3a',
  ],
  colorBackground: false,
  decimals: null,
  format: 'none',
  valueFontSize: '80%',
  valueName: 'avg',
  sparklineFull: false,
  sparklineShow: false,
  thresholds: '',
};

local initSingleStat(config) = grafana.singlestat.new(
  title=config.title,
  datasource=config.datasource,
  colors=config.colors,
  colorBackground=config.colorBackground,
  decimals=config.decimals,
  format=config.format,
  valueFontSize=config.valueFontSize,
  valueName=config.valueName,
  sparklineFull=config.sparklineFull,
  sparklineShow=config.sparklineShow,
  thresholds=config.thresholds,
);

local getSingleStat(c) = if std.objectHas(c, 'target')
then
  local config = default_singlestat + c;
  local tc = default_prometheus_target + config.target;
  local t = prometheus.target(
    tc.expr,
    legendFormat=tc.legendFormat,
    instant=tc.instant,
    intervalFactor=tc.intervalFactor,
    format=tc.format
  );
  initSingleStat(config).addTarget(t)
else
  local config = default_singlestat + c;
  initSingleStat(config);

//  _                       _       _
// | |_ ___ _ __ ___  _ __ | | __ _| |_ ___
// | __/ _ \ '_ ` _ \| '_ \| |/ _` | __/ _ \
// | ||  __/ | | | | | |_) | | (_| | ||  __/
//  \__\___|_| |_| |_| .__/|_|\__,_|\__\___|
//                   |_|
// default values from https://github.com/grafana/grafonnet-lib/blob/master/grafonnet/template.libsonnet
local template_default = {
  label: null,
  allValues: null,
  tagValuesQuery: '',
  current: null,
  hide: '',
  regex: '',
  refresh: 'never',
  includeAll: false,
  multi: false,
  sort: 0,
};
local getTemplate(c) =
  local config = template_default + c;
  grafana.template.new(
    name=config.name,
    datasource=config.datasource,
    query=config.query,
    label=config.label,
    current=config.current,
    regex=config.regex,
    refresh=config.refresh,
    sort=config.sort,
  );

//      _           _     _                         _   _ _       _
//   __| | __ _ ___| |__ | |__   ___   __ _ _ __ __| | | (_)_ __ | | __
//  / _` |/ _` / __| '_ \| '_ \ / _ \ / _` | '__/ _` | | | | '_ \| |/ /
// | (_| | (_| \__ \ | | | |_) | (_) | (_| | | | (_| | | | | | | |   <
//  \__,_|\__,_|___/_| |_|_.__/ \___/ \__,_|_|  \__,_| |_|_|_| |_|_|\_\
local link_default = {
  asDropdown: true,
  includeVars: false,
  keepTime: false,
  icon: 'external link',
  url: '',
  targetBlank: false,
  type: 'dashboards',
};

local getDashboardLink(c) =
  local config = link_default + c;
  grafana.link.dashboards(
    title=config.title,
    tags=config.tags,
    keepTime=config.keepTime,
    includeVars=config.includeVars,
  );

{
  getAnnotation(config):: getAnnotation(config),
  getDashboard(config):: getDashboard(config),
  getPanel(config):: getPanel(config),
  getRow(config):: getRow(config),
  getSingleStat(config):: getSingleStat(config),
  getTemplate(config):: getTemplate(config),
  getDashboardLink(config):: getDashboardLink(config),
}
