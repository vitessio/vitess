/**
 * This is a helper library to generate os resources reading the configuration from os_config.libsonnet
 */

local grafonnet_helper = import './grafonnet_helper.libsonnet';

{
  /**
   * Builds grapPanel using grafonnet and the configuration from `os_config.libsonnet`
   *
   * @name os_helper.getPanel
   *
   * @param `config`: The panel configuration from os_config file.
   *
   * @return A new graphPanel with the configuration specified in `os_config.libsonnet`
   *
   */
  getPanel(config):: grafonnet_helper.getPanel(config),
}
