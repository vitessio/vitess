/** This is a configuration file containing metadata for OS (Prometheus Node) grafana resources. */

local config = import '../../../config.libsonnet';
local configuration_templates = import './configuration_templates.libsonnet';
local node_ct = configuration_templates.prometheus_node;

// TODO: move local template variables and fields to ./configuration_templates.libsonnet.
{
  // TODO: add description for each panel.
  panels: {

    local vtgate_panel_template = node_ct.panel.percent_panel,
    local vttablet_host_view_panel_template = node_ct.panel.vttablet_host_view,

    CPUUsage:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'CPU Usage',
        targets: [
          {
            expr:
              |||
                1 -
                avg (
                  rate(
                    node_cpu_seconds_total{
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
    CPUUsageByInstance:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'CPU Usage',
        targets: [
          {
            expr:
              |||
                1 -
                avg by (instance)(
                  rate(
                    node_cpu_seconds_total{
                      instance=~"$host",
                      mode="idle"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    MemoryUsage:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'Memory Usage',
        targets: [
          {
            expr:
              |||
                1 -
                sum (
                  node_memory_MemAvailable_bytes{
                    %(vtgateNodeSelector)s
                  }
                )
                /
                sum (
                  node_memory_MemTotal_bytes{
                    %(vtgateNodeSelector)s
                  }
                )
              ||| % config._config,
            legendFormat: 'Memory Usage',
          },
        ],
      },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    MemoryUsageByInstance:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'Memory Usage',
        targets: [
          {
            expr:
              |||
                1 -
                sum by (instance)(
                  node_memory_MemAvailable_bytes{
                    instance=~"$host"
                  }
                )
                /
                sum by (instance)(
                  node_memory_MemTotal_bytes{
                    instance=~"$host"
                  }
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    TCPRetransmissions:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'TCP Retransmissions',
        targets: [
          {
            expr:
              |||
                sum (
                  rate(
                    node_netstat_Tcp_RetransSegs{
                      %(vtgateNodeSelector)s
                    }[1m]
                  )
                )
                /
                sum (
                  rate(
                    node_netstat_Tcp_OutSegs{
                      %(vtgateNodeSelector)s
                    }[1m]
                  )
                )
              ||| % config._config,
            legendFormat: 'TCP retransmissions',
          },
        ],
      },

    TCPRetransmissionsByInstance:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'TCP Retransmissions',
        targets: [
          {
            expr:
              |||
                sum by (instance) (
                  rate(
                    node_netstat_Tcp_RetransSegs{
                      instance=~"$host"
                    }[1m]
                  )
                )
                /
                sum by (instance) (
                  rate(
                    node_netstat_Tcp_OutSegs{
                      instance=~"$host"
                    }[1m]
                  )
                )
              |||,
            legendFormat: '{{instance}}',
          },
        ],
      },

    NetworkUsage:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
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
                    node_network_receive_bytes_total{
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
                    node_network_transmit_bytes_total{
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
    NetworkUsageByInstance:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
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
                    node_network_receive_bytes_total{
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
                    node_network_transmit_bytes_total{
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
    NetworkUsageByInstanceFilteredByShardKeyspace:
      vttablet_host_view_panel_template
      + node_ct.panel.null_as_zeros {
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
                    node_network_receive_bytes_total{
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
                * 8
              |||,
            legendFormat: 'ingress - {{instance}}',
          },
          {
            expr:
              |||
                sum by (instance)(
                  rate(
                    node_network_transmit_bytes_total{
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
                * 8
              |||,
            legendFormat: 'egress - {{instance}}',
          },
        ],
      },

    NetworkRxByInstance:
      vttablet_host_view_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'Network Rx Bytes',
        format: 'bps',
        targets: [
          {
            expr:
              |||
                sum by (instance)(
                  rate(
                    node_network_receive_bytes_total{
                      instance=~"$host"
                    }[1m]
                  )
                )
                * 8
              |||,
            legendFormat: 'ingress - {{instance}}',
          },
        ],
      },

    NetworkTxByInstance:
      vttablet_host_view_panel_template
      + node_ct.panel.null_as_zeros {
        title: 'Network Tx Bytes',
        format: 'bps',
        targets: [
          {
            expr:
              |||
                sum by (instance)(
                  rate(
                    node_network_transmit_bytes_total{
                      instance=~"$host"
                    }[1m]
                  )
                )
                * 8
              |||,
            legendFormat: 'egress - {{instance}}',
          },
        ],
      },

    DiskUsageByInstance:
      vtgate_panel_template
      + node_ct.panel.null_as_zeros {
        title: '/mnt disk free',
        min: null,
        targets: [
          {
            expr:
              |||
                avg by(instance)(
                  node_filesystem_avail_bytes{
                    instance=~"$host",
                    mountpoint="%(vttabletMountpoint)s"
                  }
                  /
                  node_filesystem_size_bytes{
                    instance=~"$host",
                    mountpoint="%(vttabletMountpoint)s"
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
