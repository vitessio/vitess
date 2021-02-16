/** This is a configuration file containing metadata for OS (Prometheus Node) grafana resources. */

local config = import '../../../config.libsonnet';
local configuration_templates = import './configuration_templates.libsonnet';

{
  // TODO: add description for each panel.
  panels: {

    local vtgate_panel_template = configuration_templates.prometheus_node.panel.percent_panel,
    local vttablet_host_view_panel_template = configuration_templates.prometheus_node.panel.vttablet_host_view,
    local keyspace_overview_panel_template = configuration_templates.prometheus_node.panel.keyspace_overview,

    CPUUsage: vtgate_panel_template {
      title: 'CPU Usage',
      targets: [
        {
          expr:
            |||
              1 -
              avg (
                rate(
                  node_cpu_seconds_total{
                    %(customCommonSelector)s,
                    %(vtgateNodeSelector)s,
                    mode="idle"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: 'cpu usage',
        },
      ],
    },


    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    CPUUsageByInstance: vtgate_panel_template {
      title: 'CPU Usage',
      targets: [
        {
          expr:
            |||
              1 -
              avg by (instance)(
                rate(
                  node_cpu_seconds_total{
                    %(customCommonSelector)s,
                    instance=~"$host",
                    mode="idle"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    CPUUsageByInstanceFilteredByShardKeyspace: vttablet_host_view_panel_template {
      title: 'CPU Usage',
      targets: [
        {
          expr:
            |||
              1 -
              avg by (instance)(
                rate(
                  node_cpu_seconds_total{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host",
                    mode="idle"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    MemoryUsage: vtgate_panel_template {
      title: 'Memory Usage',
      targets: [
        {
          expr:
            |||
              1 -
              sum (
                node_memory_MemAvailable{
                  %(customCommonSelector)s,
                  %(vtgateNodeSelector)s
                }
              )
              /
              sum (
                node_memory_MemTotal{
                  %(customCommonSelector)s,
                  %(vtgateNodeSelector)s
                }
              )
            ||| % config._config,
          legendFormat: 'Memory Usage',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    MemoryUsageByInstance: vtgate_panel_template {
      title: 'Memory Usage',
      targets: [
        {
          expr:
            |||
              1 -
              sum by (instance)(
                node_memory_MemAvailable{
                  %(customCommonSelector)s,
                  instance=~"$host"
                }
              )
              /
              sum by (instance)(
                node_memory_MemTotal{
                  %(customCommonSelector)s,
                  instance=~"$host"
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    MemoryUsageByInstanceFilteredByShardKeyspace: vttablet_host_view_panel_template {
      title: 'Memory Usage',
      targets: [
        {
          expr:
            |||
              1 -
              sum by (instance)(
                node_memory_MemAvailable{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  shard=~"$shard",
                  instance=~"$host"
                }
              )
              /
              sum by (instance)(
                node_memory_MemTotal{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  shard=~"$shard",
                  instance=~"$host"
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    TCPRetransmissions: vtgate_panel_template {
      title: 'TCP Retransmissions',
      targets: [
        {
          expr:
            |||
              sum (
                irate(
                  node_netstat_Tcp_RetransSegs{
                    %(customCommonSelector)s,
                    %(vtgateNodeSelector)s
                  }[1m]
                )
              )
              /
              sum (
                irate(
                  node_netstat_Tcp_OutSegs{
                    %(customCommonSelector)s,
                    %(vtgateNodeSelector)s
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: 'TCP retransmissions',
        },
      ],
    },

    TCPRetransmissionsByInstance: vtgate_panel_template {
      title: 'TCP Retransmissions',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                irate(
                  node_netstat_Tcp_RetransSegs{
                    %(customCommonSelector)s,
                    instance=~"$host"
                  }[1m]
                )
              )
              /
              sum by (instance) (
                irate(
                  node_netstat_Tcp_OutSegs{
                    %(customCommonSelector)s,
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    NetworkUsage: vtgate_panel_template {
      title: 'Network Usage',
      format: 'bps',
      min: null,
      seriesOverrides: [
        {
          alias: '/egress .*/',
          transform: 'negative-Y',
        },
      ],
      targets: [
        {
          expr:
            |||
              sum (
                rate(
                  node_network_receive_bytes{
                    %(customCommonSelector)s,
                    %(vtgateNodeSelector)s
                  }[5m]
                )
              )
              * 8
            ||| % config._config,
          legendFormat: 'ingress',
        },
        {
          expr:
            |||
              sum (
                rate(
                  node_network_transmit_bytes{
                    %(customCommonSelector)s,
                    %(vtgateNodeSelector)s
                  }[5m]
                )
              )
              * 8
            ||| % config._config,
          legendFormat: 'egress',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    NetworkUsageByInstance: vtgate_panel_template {
      title: 'Network Usage',
      format: 'Bps',
      min: null,
      seriesOverrides: [
        {
          alias: '/egress .*/',
          transform: 'negative-Y',
        },
      ],
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  node_network_receive_bytes{
                    %(customCommonSelector)s,
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: 'ingress - {{instance}}',
        },
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  node_network_transmit_bytes{
                    %(customCommonSelector)s,
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: 'egress - {{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    NetworkUsageByInstanceFilteredByShardKeyspace: vttablet_host_view_panel_template {
      title: 'Network Usage',
      format: 'bps',
      min: null,
      seriesOverrides: [
        {
          alias: '/egress .*/',
          transform: 'negative-Y',
        },
      ],
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  node_network_receive_bytes{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              * 8
            ||| % config._config,
          legendFormat: 'ingress - {{instance}}',
        },
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  node_network_transmit_bytes{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              * 8
            ||| % config._config,
          legendFormat: 'egress - {{instance}}',
        },
      ],
    },

    NetworkRxByInstanceFilteredByShardKeyspace: vttablet_host_view_panel_template {
      title: 'Network Rx Bytes',
      format: 'bps',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  node_network_receive_bytes{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              * 8
            ||| % config._config,
          legendFormat: 'ingress - {{instance}}',
        },
      ],
    },

    NetworkTxByInstanceFilteredByShardKeyspace: vttablet_host_view_panel_template {
      title: 'Network Tx Bytes',
      format: 'bps',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  node_network_transmit_bytes{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              * 8
            ||| % config._config,
          legendFormat: 'egress - {{instance}}',
        },
      ],
    },

    CPUUsageByKeyspace: keyspace_overview_panel_template {
      title: 'CPU Usage',
      targets: [
        {
          expr:
            |||
              1 -
              avg by (keyspace)(
                rate(
                  node_cpu_seconds_total{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    mode="idle"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{keyspace}}',
        },
      ],
    },

    MemoryUsageByKeyspace: keyspace_overview_panel_template {
      title: 'Memory Usage',
      targets: [
        {
          expr:
            |||
              1 -
              avg by (keyspace)(
                node_memory_MemAvailable{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
                /
                node_memory_MemTotal{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
            ||| % config._config,
          legendFormat: '{{keyspace}}',
        },
      ],
    },

    NetworkUsageByKeyspace: keyspace_overview_panel_template {
      title: 'Network Usage',
      decimalsY1: null,
      fill: 1,
      format: 'bps',
      min: null,
      seriesOverrides: [
        {
          alias: '/egress .*/',
          transform: 'negative-Y',
        },
      ],
      targets: [
        {
          expr:
            |||
              sum  by (keyspace)(
                rate(
                  node_network_receive_bytes{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }[5m]
                )
              ) * 8
            ||| % config._config,
          legendFormat: 'ingress {{keyspace}}',
        },
        {
          expr:
            |||
              sum by (keyspace)(
                rate(
                  node_network_transmit_bytes{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }[5m]
                )
              ) * 8
            ||| % config._config,
          legendFormat: 'egress {{keyspace}}',
        },
      ],
    },

    DiskUsageByKeyspace: keyspace_overview_panel_template {
      title: '/mnt disk free',
      targets: [
        {
          expr:
            |||
              avg by(keyspace)(
                node_filesystem_avail{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    mountpoint="/mnt"
                }
                /
                node_filesystem_size{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  mountpoint="/mnt"
                }
              )
            ||| % config._config,
          legendFormat: '{{keyspace}}',
          intervalFactor: 1,
        },
      ],
    },
    DiskUsageByInstanceFilteredByShardKeyspace: vtgate_panel_template {
      title: '/mnt disk free',
      min: null,
      targets: [
        {
          expr:
            |||
              avg by(instance)(
                node_filesystem_avail{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  shard=~"$shard",
                  instance=~"$host",
                  mountpoint="/mnt"
                }
                /
                node_filesystem_size{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  shard=~"$shard",
                  instance=~"$host",
                  mountpoint="/mnt"
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
          intervalFactor: 1,
        },
      ],
    },
  },
}
