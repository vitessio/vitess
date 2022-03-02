// TODO deduplicate helper code. Same/similar functions are used by vtgate, vttablet and orchestrator helpers
{
  dashboard:: import 'dashboard_helper.libsonnet',
  default:: import 'grafonnet_helper.libsonnet',
  mysql:: import 'mysql_helper.libsonnet',
  os:: import 'os_helper.libsonnet',
  vtgate:: import 'vtgate_helper.libsonnet',
  vttablet:: import 'vttablet_helper.libsonnet',
}
