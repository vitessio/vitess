/**
 * This is a helper library to generate os resources reading the configuration from mysql_config.libsonnet
 */

local grafonnet_helper = import './grafonnet_helper.libsonnet';

/**
 * Builds grapPanel using grafonnet and the configuration from `mysql_config.libsonnet`
 *
 * @name mysql_helper.getPanel
 *
 * @param `config`: The panel configuration from mysql_config file.
 *
 * @return A new graphPanel with the configuration specified in `mysql_config.libsonnet`
 *
 */
{
  getPanel(config):: grafonnet_helper.getPanel(config),
}
