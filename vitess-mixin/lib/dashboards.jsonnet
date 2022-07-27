local dashboards = (import '../mixin.libsonnet').grafanaDashboards;

local config = (import '../config.libsonnet');
local deploy_list = config._config.deploy_list;

{
  [name]: dashboards[name]
  for name in std.objectFields(dashboards)
  if std.member(dashboards[name].environments, std.extVar('env'))
}
