/**
 * This is a helper library to generate vttablet resources reading the configuration from `vttablet_config.libsonnet`
 */
local grafonnet_helper = import './grafonnet_helper.libsonnet';

{
  /**
  * Builds grapPanel using grafonnet and the configuration from `vttablet_config.libsonnet`
  *
  * @name vttablet_helper.getPanel
  *
  * @param `config`: The panel configuration from vttablet_config file.
  *
  * @return A new graphPanel with the configuration specified in `vttablet_config.libsonnet`
  *
  */
  getPanel(config):: grafonnet_helper.getPanel(config),
  /**
  * Builds singlestat using grafonnet and the configuration from `vttablet_config.libsonnet`
  *
  * @name vttablet_helper.getPanel
  *
  * @param `config`: The singlestat configuration from vttablet_config file.
  *
  * @return A new singlestat with the configuration specified in `vttablet_config.libsonnet`
  *
  */
  getSingleStat(config):: grafonnet_helper.getSingleStat(config),
}
