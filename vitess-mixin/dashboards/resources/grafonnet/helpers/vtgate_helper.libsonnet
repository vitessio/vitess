/**
 * This is a helper library to generate vtgate resources reading the configuration from vtgate_config.libsonnet
 */

local grafonnet_helper = import './grafonnet_helper.libsonnet';

{
  /**
   * Builds grapPanel using grafonnet and the configuration from `vtgate_config.libsonnet`
   *
   * @name vtgate_helper.getPanel
   *
   * @param `config`: The panel configuration from vtgate_config file.
   *
   * @return A new graphPanel with the configuration specified in `vtgate_config.libsonnet`
   *
   */
  getPanel(config):: grafonnet_helper.getPanel(config),

  /**
   * Builds a singlestat using grafonnet and the configuration from `vtgate_config.libsonnet`
   *
   * @name vtgate_helper.getSingleStat
   *
   * @param `config`: The singlestat configuration from vtgate_config file.
   *
   * @return A new singlestat with the configuration specified in `vtgate_config.libsonnet`
   *
   */
  getSingleStat(config):: grafonnet_helper.getSingleStat(config),
}
