/** This is a helper library to load grafonnet dashboards using the mixin metadata stored in `config.libshonnet` */

local grafonnet_helper = import 'grafonnet_helper.libsonnet';

{
  /**
  * Builds a dashboard using grafonnet and the configuration from `config.libsonnet`
  *
  * @name dashboard_helper.getDashboard
  *
  * @param config The dashboard configuration from mixin config file.
  * @return A new graphPanel with the configuration specified in `config.libsonnet`
  *
  */
  getDashboard(config):: grafonnet_helper.getDashboard(config),
}
