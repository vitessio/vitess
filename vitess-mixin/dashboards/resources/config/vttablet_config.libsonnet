/** This is a configuration file containing metadata for vttablet grafana resources. */

local config = import '../../../config.libsonnet';
local configuration_templates = import './configuration_templates.libsonnet';
local vitess_ct = configuration_templates.prometheus_vitess;

{
  panels: {
    //  ____                  _
    // |  _ \ __ _ _ __   ___| |___
    // | |_) / _` | '_ \ / _ \ / __|
    // |  __/ (_| | | | |  __/ \__ \
    // |_|   \__,_|_| |_|\___|_|___/

    // TODO: add description for each panel.
    // TODO: move local template variables to ./configurations_templates.libsonnet.

    //Override default_panel values with custom configuration
    local vttablet_queries_killed = vitess_ct.panel.legend_min_max_avg + vitess_ct.panel.null_as_zeros,
    local vttablet_query_errors_by_type = vitess_ct.panel.legend_min_max_avg + vitess_ct.panel.null_as_zeros,

    local panel_template = vitess_ct.panel.legend_min_max_current {
      legend_sort: 'current',
      legend_sortDesc: true,
      shared_tooltip: true,
      sort: 'decreasing',
    },

    local vttablet_host_view_panel_template = panel_template {
      legend_sort: 'avg',
      legend_avg: true,
      legend_current: false,
    },

    //TODO Create a recording rule.
    countServingTabletsByKeyspaceShard: panel_template {
      title: '# of serving tablets (by keyspace/shard)',
      legend_sortDesc: false,
      shared_tooltip: false,
      sort: 'increasing',
      targets: [
        {
          expr:
            |||
              count(
                vttablet_tablet_server_state{
                  %(customCommonSelector)s,
                  %(vttabletSelector)s,
                  name="SERVING"
                }
              ) by (keyspace, shard)
            |||
            % config._config,
          legendFormat: '{{keyspace}}/{{shard}}',
        },
      ],
    },

    vttabletRequestsByTable: panel_template {
      title: 'Requests (by table)',
      format: 'rps',
      targets: [
        {
          expr:
            |||
              sum by (table)(
                vitess_mixin:vttablet_query_counts_byregion_keyspace_table:irate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
              or
              vector(0)
            ||| % config._config,
          legendFormat: '{{table}}',
        },
      ],
    },

    vttabletRequestsByPlanType: panel_template {
      title: 'Requests (by plan type)',
      format: 'rps',
      targets: [
        {
          expr:
            |||
              sum by (plan)(
                vitess_mixin:vttablet_query_counts_byregion_keyspace_table_plan:irate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
              or
              vector(0)
            ||| % config._config,
          legendFormat: '{{plan}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMEHTEUS TARGET
    vttabletRequestsByPlanTypeFilteredByShardKeyspace: panel_template {
      title: 'Requests (by plan type)',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum by (plan_type)(
                rate(
                  vttablet_queries_count{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  } [5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{plan_type}}',
        },
      ],
    },

    vttabletRequestsByKeyspace: panel_template {
      title: 'Requests',
      format: 'ops',
      legend_current: false,
      legend_avg: true,
      targets: [
        {
          expr:
            |||
              sum by (keyspace)(
                vitess_mixin:vttablet_query_counts_byregion_keyspace:irate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
            ||| % config._config,
          legendFormat: '{{keyspace}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletRequestsByInstance: panel_template {
      title: 'Requests',
      format: 'ops',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      targets: [
        {
          expr:
            |||
              sum  by (instance)(
                rate(
                  vttablet_query_counts{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletRequestsByShardFilteredByInstanceKeyspace: panel_template {
      title: 'Requests (by shard)',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum  by (shard)(
                rate(
                  vttablet_query_counts{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
          intervalFactor: 1,
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletRequestsByTableFilteredByInstanceShardKeyspace: panel_template {
      title: 'Requests (by table)',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum  by (table)(
                rate(
                  vttablet_query_counts{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{table}}',
          intervalFactor: 1,
        },
      ],
    },


    vttabletRequestsSuccessRateFilterByKeyspace: panel_template {
      title: 'Requests success rate (by keyspace)',
      format: 'percent',
      max: 100,
      min: null,
      fill: 1,
      targets: [
        {
          expr:
            |||
              100 -
              (
                100
                *
                sum(
                  vitess_mixin:vttablet_errors_byregion_keyspace:irate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
                /
                sum(
                  vitess_mixin:vttablet_query_counts_byregion_keyspace:irate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: 'Success rate',
        },
      ],
    },

    vttabletErrorRateByKeyspace: panel_template {
      title: 'Error rate',
      format: 'percentunit',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      nullPointMode: 'null as zero',
      targets: [
        {
          expr:
            |||
              sum by (keyspace)(
                vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table:irate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                })
              /
              (
                sum by (keyspace)(
                  vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table:irate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
                +
                sum by (keyspace)(
                  vitess_mixin:vttablet_query_counts_byregion_keyspace:irate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{keyspace}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletErrorRateByInstance: panel_template {
      title: 'Error rate',
      format: 'percentunit',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      nullPointMode: 'null as zero',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_query_error_counts{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                ) > 0
              )
              /
              (
                sum by (instance)(
                  rate(
                    vttablet_query_error_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  ) > 0
                )
                +
                sum by (instance)(
                  rate(
                    vttablet_query_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    vttabletErrorRateByPlanFilteredByInstanceShardKeyspace: panel_template {
      title: 'Error rate (by plan type)',
      format: 'percentunit',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      nullPointMode: 'null as zero',
      targets: [
        {
          expr:
            |||
              sum by (plan)(
                rate(
                  vttablet_query_error_counts{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                ) > 0
              )
              /
              (
                sum by (plan)(
                  rate(
                    vttablet_query_error_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  ) > 0
                )
                +
                sum by (plan)(
                  rate(
                    vttablet_query_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{plan}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletErrorRateByShardFilteredByInstanceKeyspace: panel_template {
      title: 'Error rate (by shard)',
      format: 'percentunit',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      nullPointMode: 'null as zero',
      targets: [
        {
          expr:
            |||
              sum by (shard)(
                rate(
                  vttablet_query_error_counts{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                ) > 0
              )
              /
              (
                sum by (shard)(
                  rate(
                    vttablet_query_error_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  ) > 0
                )
                +
                sum by (shard)(
                  rate(
                    vttablet_query_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROM TARGET
    vttabletErrorRateByTableFilteredByInstanceShardKeyspace: panel_template {
      title: 'Error rate (by table)',
      format: 'percentunit',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      nullPointMode: 'null as zero',
      targets: [
        {
          expr:
            |||
              sum by (table)(
                rate(
                  vttablet_query_error_counts{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                ) > 0
              )
              /
              (
                sum by (table)(
                  rate(
                    vttablet_query_error_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  ) > 0
                )
                +
                sum by (table)(
                  rate(
                    vttablet_query_counts{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{table}}',
        },
      ],
    },

    vttabletErrorByTablesFilterByKeyspaceTables: panel_template {
      title: 'Errors (by table)',
      format: 'cps',
      targets: [
        {
          expr:
            |||
              sum  by (table)(
                vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table:irate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
            ||| % config._config,
          legendFormat: '{{table}}',
        },
      ],
    },

    vttabletErrorByPlanFilterByKeyspaceTables: panel_template {
      title: 'Errors (by plan type/table)',
      format: 'cps',
      targets: [
        {
          expr:
            |||
              sum  by (plan,table)(
                vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table_plan:irate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
            ||| % config._config,
          legendFormat: '{{plan}} / {{table}}',
        },
      ],
    },

    vttabletErrorByShardFilterByKeyspaceTables: panel_template {
      title: 'Errors (by shard/table)',
      format: 'cps',
      targets: [
        {
          expr:
            |||
              sum by (shard,table)(
                vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table_shard:irate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
            ||| % config._config,
          legendFormat: '{{shard}} / {{table}}',
        },
      ],
    },

    //Use recording rule
    vttabletRowsReturnedByTablesFilterByKeyspace: panel_template {
      title: 'Rows returned (by table)',
      targets: [
        {
          expr:
            |||
              sum by (table)(
                vitess_mixin:vttablet_query_row_counts_byregion_keyspace_table:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
            ||| % config._config,
          legendFormat: '{{table}}',
        },
      ],
    },

    vttabletRowsReturnedByPlansFilterByKeyspace: panel_template {
      title: 'Rows returned (by plan type)',
      targets: [
        {
          expr:
            |||
              sum by (plan)(
                vitess_mixin:vttablet_query_row_counts_byregion_keyspace_table_plan:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
            ||| % config._config,
          legendFormat: '{{plan}}',
        },
      ],
    },

    vttabletRowsReturnedByShardFilterByKeyspace: panel_template {
      title: 'Rows returned (by shard)',
      targets: [
        {
          expr:
            |||
              sum by (shard)(
                vitess_mixin:vttablet_query_row_counts_byregion_keyspace_table_shard:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace",
                  table=~"$table"
                }
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    //TODO use recording rule `vitess_mixin_79` when deployed.
    vttabletRowsReturnedByTableFilteredByKeyspaceShardInstance: self.vttabletRowsReturnedByTablesFilterByKeyspace {
      targets: [
        {
          expr:
            |||
              sum by (table) (
                rate(vttablet_query_row_counts{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{table}}',
        },
      ],
    },

    //TODO use recording rule `vitess_mixin_78` when deployed.
    vttabletRowsReturnedByPlansFilterByKeyspaceShardInstance: self.vttabletRowsReturnedByPlansFilterByKeyspace {
      targets: [
        {
          expr:
            |||
              sum by (plan) (
                rate(vttablet_query_row_counts{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{plan}}',
        },
      ],
    },

    vttabletQueryDurationAvg: panel_template {
      title: 'Query duration (avg)',
      format: 's',
      targets: [
        {
          expr:
            |||
              sum(
                vitess_mixin:vttablet_queries_sum_byregion_keyspace:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"}
                )
              /
              sum (
                vitess_mixin:vttablet_queries_count_byregion_keyspace:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              sum by (shard)(
                vitess_mixin:vttablet_queries_sum_byregion_keyspace_shard:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
              /
              sum by(shard)(
                vitess_mixin:vttablet_queries_count_byregion_keyspace_shard:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletQueryDurationP50: panel_template {
      title: 'Query duration (p50)',
      format: 's',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.50,
                sum by (le)(
                  vitess_mixin:vttablet_queries_bucket_byregion_keyspace:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              histogram_quantile(
                0.50,
                sum by (shard, le)(
                  vitess_mixin:vttablet_queries_bucket_byregion_keyspace_shard:rate1m{
                    region="$region",
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletQueryDurationP95: panel_template {
      title: 'Query duration (p95)',
      format: 's',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.95,
                sum by (le)(
                  vitess_mixin:vttablet_queries_bucket_byregion_keyspace:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              histogram_quantile(
                0.95,
                sum by (shard, le)(
                  vitess_mixin:vttablet_queries_bucket_byregion_keyspace_shard:rate1m{
                    region="$region",
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletQueryDurationP999: panel_template {
      title: 'Query duration (p999)',
      format: 's',

      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.999,
                sum by (le)(
                  vitess_mixin:vttablet_queries_bucket_byregion_keyspace:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              histogram_quantile(
                0.999,
                sum by (shard, le)(
                  vitess_mixin:vttablet_queries_bucket_byregion_keyspace_shard:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletQueryDurationP99ByKeyspace: panel_template {
      title: 'Duration (p99)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.99,sum by(keyspace,le)(
                  vitess_mixin:vttablet_queries_bucket_byregion_keyspace:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{keyspace}}',
        },
      ],
    },

    //TODO DEDUPLICATE LEGEND CONFIGURATION FOR QUERY DURATION PANELS
    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationAvgByInstance: panel_template {
      title: 'Query Duration (avg)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'max',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  vttablet_queries_sum{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              /
              sum by(instance)(
                rate(
                  vttablet_queries_count{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP50ByInstance: panel_template {
      title: 'Query Duration (p50)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'max',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.50,sum by(instance,le)(
                  rate(
                    vttablet_queries_bucket{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP95ByInstance: panel_template {
      title: 'Query Duration (p95)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'max',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.95,sum by(instance,le)(
                  rate(
                    vttablet_queries_bucket{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP99ByInstance: panel_template {
      title: 'Duration (p99)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.99,sum by(instance,le)(
                  rate(
                    vttablet_queries_bucket{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP99ByPlan: panel_template {
      title: 'Duration p99 (by plan type)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.99,sum by(plan_type,le)(
                  rate(
                    vttablet_queries_bucket{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{plan_type}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryDurationP99ByShard: panel_template {
      title: 'Duration p99 (by shard)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'avg',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.99,sum by(shard,le)(
                  rate(
                    vttablet_queries_bucket{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletTransDurationAvg: panel_template {
      title: 'Transaction duration (avg)',
      format: 's',
      targets: [
        {
          expr:
            |||
              sum(
                vitess_mixin:vttablet_transactions_sum_byregion_keyspace:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
              /
              sum (
                vitess_mixin:vttablet_transactions_count_byregion_keyspace:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              sum by (shard)(
                vitess_mixin:vttablet_transactions_sum_byregion_keyspace_shard:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"
                }
              )
              /
              sum by (shard)(
                vitess_mixin:vttablet_transactions_count_byregion_keyspace_shard:rate1m{
                  %(customCommonSelector)s,
                  keyspace="$keyspace"}
                )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletTransDurationP50: panel_template {
      title: 'Transaction duration (p50)',
      format: 's',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.50,
                sum by (le)(
                  vitess_mixin:vttablet_transactions_bucket_byregion_keyspace:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              histogram_quantile(
                0.50,
                sum by (le,shard)(
                  vitess_mixin:vttablet_transactions_bucket_byregion_keyspace_shard:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletTransDurationP95: panel_template {
      title: 'Transaction duration (p95)',
      format: 's',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.95,
                sum by (le)(
                  vitess_mixin:vttablet_transactions_bucket_byregion_keyspace:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              histogram_quantile(
                0.95,
                sum by (le,shard)(
                  vitess_mixin:vttablet_transactions_bucket_byregion_keyspace_shard:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    vttabletTransDurationP999: panel_template {
      title: 'Transaction duration (p999)',
      format: 's',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.999,
                sum by (le)(
                  vitess_mixin:vttablet_transactions_bucket_byregion_keyspace:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: 'Total',
        },
        {
          expr:
            |||
              histogram_quantile(
                0.999,
                sum by (le,shard)(
                  vitess_mixin:vttablet_transactions_bucket_byregion_keyspace_shard:rate1m{
                    %(customCommonSelector)s,
                    keyspace="$keyspace"
                  }
                )
              )
            ||| % config._config,
          legendFormat: '{{shard}}',
        },
      ],
    },

    //TODO DEDUPLICATE LEGEND CONFIGURATION FOR TRANSACTION DURATION PANELS
    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionDurationAvgByInstance: panel_template {
      title: 'Transaction Duration (avg)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'max',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  vttablet_transactions_sum{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              /
              sum by(instance)(
                rate(
                  vttablet_transactions_count{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionDurationP50ByInstance: panel_template {
      title: 'Transaction Duration (p50)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'max',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.50,sum by(instance,le)(
                  rate(
                    vttablet_transactions_bucket{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionDurationP95ByInstance: panel_template {
      title: 'Transaction Duration (p95)',
      format: 's',
      legend_current: false,
      legend_avg: true,
      legend_sort: 'max',
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.95,sum by(instance,le)(
                  rate(
                    vttablet_transactions_bucket{
                      %(customCommonSelector)s,
                      keyspace="$keyspace",
                      shard=~"$shard",
                      instance=~"$host"
                    }[5m]
                  )
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    vttabletQueryTransactionKilledByKeyspaceShard: panel_template {
      title: 'Query/Transaction killed (by keyspace/shard)',
      format: 'cps',
      legend_alignAsTable: true,
      nullPointMode: 'null as zero',
      shared_tooltip: false,
      targets: [
        {
          expr:
            |||
              sum by (keyspace,shard)(
                vitess_mixin:vttablet_kills_byregion_keyspace_shard:irate1m{
                  %(customCommonSelector)s
                }
              ) > 0
            ||| % config._config,
          legendFormat: '{{keyspace}}/{{shard}}',
        },
      ],
    },

    vttabletRestart: {
      title: 'vttablet (by keyspace/shard)',
      bars: true,
      datasource: '%(dataSource)s' % config._config,
      fill: 0,
      format: 'short',
      legend_values: true,
      legend_alignAsTable: true,
      legend_max: true,
      legend_sort: 'max',
      legend_sortDesc: false,
      lines: false,
      min: 0,
      shared_tooltip: false,
      sort: 'increasing',
      targets: [
        {
          expr:
            |||
              sum by (keyspace,shard) (
                vitess_mixin:process_start_time_seconds_byregion_keyspace_shard_job:sum5m{
                  %(customCommonSelector)s,
                  %(vttabletSelector)s
                }
              ) > 0
            ||| % config._config,
          legendFormat: '{{keyspace}}/{{shard}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolAvailableConnections: vttablet_host_view_panel_template {
      title: 'Available Connections',
      description: 'number of available connections in the pool in real-time',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                vttablet_conn_pool_available{
                  %(customCommonSelector)s,
                  keyspace='$keyspace',
                  shard=~'$shard',
                  instance=~'$host'
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolActiveConnections: vttablet_host_view_panel_template {
      title: 'Active Connections',
      description: 'count of in use connections to mysql',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by(instance) (
                vttablet_conn_pool_active{
                  %(customCommonSelector)s,
                  keyspace='$keyspace',
                  shard=~'$shard',
                  instance=~'$host'
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolIddleClosedRate: vttablet_host_view_panel_template {
      title: 'Idle Closed Rate',
      description: 'rate of closing connections due to the idle timeout',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_conn_pool_idle_closed{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolWaitCount: vttablet_host_view_panel_template {
      title: 'Wait count',
      description: 'WaitCount will give you how often the transaction pool gets full that causes new transactions to wait.',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_conn_pool_wait_count{
                    %(customCommonSelector)s,
                    keyspace='$keyspace',
                    shard=~'$shard',
                    instance=~'$host'
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletQueryPoolAvgWaitTime: vttablet_host_view_panel_template {
      title: 'Avg wait time',
      format: 's',
      description: 'WaitTime/WaitCount will tell you the average wait time.',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_conn_pool_wait_time{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              /
              sum by (instance) (
                rate(
                  vttablet_conn_pool_wait_count{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    vttabletQueriesKilled: vttablet_queries_killed {
      title: 'Queries Killed',
      description: |||
        Kills reports the queries and transactions killed by VTTablet due to timeout.
        It’s a very important variable to look at during outages.
      |||,
      targets: [
        {
          expr: |||
            sum by (instance)(
              vitess_mixin:vttablet_kills:irate1m{
                %(customCommonSelector)s,
                keyspace=~"$keyspace",
                shard=~"$shard",
                instance=~"$host"
              } > 0
            )
          ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    vttabletQueryErrorsByType: vttablet_query_errors_by_type {
      title: 'Query errors by type',
      description: '',
      targets: [
        {
          expr: |||
            sum by (error_code)(
              vitess_mixin:vttablet_errors:irate1m{
                keyspace=~"$keyspace",
                shard=~"$shard",
                instance=~"$host",
                region=~"$region"
              } > 0
            )
          ||| % config._config,
          legendFormat: 'ErrorCode: {{error_code}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolAvailableConnections: vttablet_host_view_panel_template {
      title: 'Available Connections',
      description: 'number of available connections in the pool',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                vttablet_transaction_pool_available{
                  %(customCommonSelector)s,
                  keyspace='$keyspace',
                  shard=~'$shard',
                  instance=~'$host'
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolActiveConnections: vttablet_host_view_panel_template {
      title: 'Active Connections',
      description: 'Number of connections actually open to mysql',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by(instance) (
                vttablet_transaction_pool_active{
                  %(customCommonSelector)s,
                  keyspace='$keyspace',
                  shard=~'$shard',
                  instance=~'$host'
                }
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolIddleClosedRate: vttablet_host_view_panel_template {
      title: 'Idle Closed Rate',
      description: 'Rate of closing connections due to the idle timeout',
      format: 'ops',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_transaction_pool_idle_closed{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolWaitCount: vttablet_host_view_panel_template {
      title: 'Wait count',
      description: 'WaitCount will give you how often the transaction pool gets full that causes new transactions to wait.',
      format: 'short',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vttablet_transaction_pool_wait_count{
                    %(customCommonSelector)s,
                    keyspace='$keyspace',
                    shard=~'$shard',
                    instance=~'$host'
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletTransactionPoolAvgWaitTime: vttablet_host_view_panel_template {
      title: 'Avg wait time',
      format: 's',
      description: 'WaitTime/WaitCount will tell you the average wait time.',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_transaction_pool_wait_time{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
              /
              sum by (instance) (
                rate(
                  vttablet_transaction_pool_wait_count{
                    %(customCommonSelector)s,
                    keyspace="$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[5m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletGarbageCollectionCount: vitess_ct.panel.go_gc_ops {
      title: 'GC Count',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  go_gc_duration_seconds_count{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace", 
                    shard=~"$shard", 
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
          intervalFactor: 1,
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletGarbageCollectionDuration: vitess_ct.panel.go_gc_seconds {
      title: 'GC Duration total per seconds',
      description: 'A summary of the pause duration of garbage collection cycles',
      targets: [
        {
          expr:
            |||
              sum by(instance)(
                rate(
                  go_gc_duration_seconds_count{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace", 
                    shard=~"$shard", 
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
          intervalFactor: 1,
        },
      ],
    },
    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletGarbageCollectionDurationQuantiles: vitess_ct.panel.go_gc_seconds {
      title: 'GC Duration quantiles (all hosts)',
      targets: [
        {
          expr:
            |||
              sum by(quantile)(
                rate(
                  go_gc_duration_seconds{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace", 
                    shard=~"$shard", 
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: 'p{{quantile}}',
          intervalFactor: 1,
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletMysqlTimeAvgFilteredByKeyspaceShardInstance: vitess_ct.panel.mysql_timings {
      title: 'MySQL time (avg)',
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_mysql_sum{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[1m]
                )
              ) 
              /
              sum by (instance) (
                rate(
                  vttablet_mysql_count{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletMysqlExecTimeP50FilterebyKeyspaceShardInstance: vitess_ct.panel.mysql_timings {
      title: 'MySQL Exec Time P50',
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.50,
              sum by (le, instance) (
                rate(
                  vttablet_mysql_bucket{
                    %(customCommonSelector)s,
                    operation="Exec",
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[1m]
                )
              )
            ) 
          ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletMysqlExecTimeP95FilterebyKeyspaceShardInstance: vitess_ct.panel.mysql_timings {
      title: 'MySQL Exec Time P95',
      targets: [
        {
          expr: |||
            histogram_quantile(
              0.95,
              sum by (le, instance) (
                rate(
                  vttablet_mysql_bucket{
                    %(customCommonSelector)s,
                    operation="Exec",
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    instance=~"$host"
                  }[1m]
                )
              )
            ) 
          ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletConsolidationRateFilteredByKeyspaceShardInstance: vitess_ct.panel.consolidation_timings_ops {
      title: 'Consolidations rate',
      description: |||
        Waits is a histogram variable that tracks various waits in the system.
        Right now, the only category is "Consolidations".
        A consolidation happens when one query waits for the results of an identical query already executing,
        thereby saving the database from performing duplicate work.
      |||,
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_waits_count{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    type="Consolidations",
                    instance=~"$host"}[1m]))
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletConsolidationWaitTimeAvgFilteredByKeyspaceShardInstance: vitess_ct.panel.consolidation_timings_seconds {
      title: 'Consolidations wait time (avg)',
      description: |||
        Waits is a histogram variable that tracks various waits in the system.
        Right now, the only category is "Consolidations".
        A consolidation happens when one query waits for the results of an identical query already executing,
        thereby saving the database from performing duplicate work.
      |||,
      targets: [
        {
          expr:
            |||
              sum by (instance) (
                rate(
                  vttablet_waits_sum{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    type="Consolidations",
                    instance=~"$host"
                  }[1m]
                )
              )
              /
              sum by (instance) (
                rate(
                  vttablet_waits_count{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard=~"$shard",
                    type="Consolidations",
                    instance=~"$host"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vttabletConsolidationWaitTimeP95FilteredByKeyspaceShardInstance: vitess_ct.panel.consolidation_timings_seconds {
      title: 'Consolidations wait time (p95)',
      description: |||
        Waits is a histogram variable that tracks various waits in the system.
        Right now, the only category is "Consolidations".
        A consolidation happens when one query waits for the results of an identical query already executing,
        thereby saving the database from performing duplicate work.
      |||,
      targets: [
        {
          expr:
            |||
              histogram_quantile(
                0.95,
                sum by (le, instance)(
                  rate(
                    vttablet_waits_bucket{
                      %(customCommonSelector)s,
                      keyspace=~"$keyspace",
                      shard=~"$shard",
                      type="Consolidations",
                      instance=~"$host"
                    }[1m]
                  )
                )
              ) 
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vtgateToVtTabletCallTimeAvgFilteredByKeyspaceShardInstance: vitess_ct.panel.vtgate_to_vttablet_calls {
      title: 'VtGate -> VtTablet Call Time (avg)',
      targets: [
        {
          expr:
            |||
              sum by (instance)(
                rate(
                  vtgate_vttablet_call_sum{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard_name=~"$shard"
                  }[1m]
                )
              )
              /
              sum by (instance)(
                rate(
                  vtgate_vttablet_call_count{
                    %(customCommonSelector)s,
                    keyspace=~"$keyspace",
                    shard_name=~"$shard"
                  }[1m]
                )
              )
            ||| % config._config,
          legendFormat: '{{instance}}',
        },
      ],
    },

    //TODO CREATE A RECORDING RULE FOR THIS PROMETHEUS TARGET
    vtgateToVtTabletCallTimeAvgFilteredByKeyspaceShardDBType: vitess_ct.panel.vtgate_to_vttablet_calls {
      title: 'VtGate -> VtTablet Call Time by Shard (avg)',
      targets: [
        {
          expr: |||
            sum by (shard_name, db_type)(
              rate(
                vtgate_vttablet_call_sum{
                  %(customCommonSelector)s,
                  keyspace=~"$keyspace",
                  shard_name=~"$shard"
                }[1m]
              )
            ) / 
            sum by (shard_name, db_type)(
              rate(
                vtgate_vttablet_call_count{
                  %(customCommonSelector)s,
                  keyspace=~"$keyspace",
                  shard_name=~"$shard"
                }[1m]
              )
            )
          ||| % config._config,
          legendFormat: '{{shard_name}}-{{db_type}}',
        },
      ],
    },
  },

  singlestats: {
    //      _             _           _        _
    //  ___(_)_ __   __ _| | ___  ___| |_ __ _| |_ ___
    // / __| | '_ \ / _` | |/ _ \/ __| __/ _` | __/ __|
    // \__ \ | | | | (_| | |  __/\__ \ || (_| | |_\__ \
    // |___/_|_| |_|\__, |_|\___||___/\__\__,_|\__|___/
    //              |___/

    vttabletQPS: {
      title: 'QPS - vttablet',
      datasource: '%(dataSource)s' % config._config,
      format: 'short',
      valueFontSize: '70%',
      valueName: 'current',
      sparklineFull: true,
      sparklineShow: true,
      target:
        {
          expr: |||
            sum by (region)(
              vitess_mixin:vttablet_query_counts_byregion:irate1m{
                %(customCommonSelector)s
              }
            )
          ||| % config._config,
          intervalFactor: 1,
        },
    },
  },
}
