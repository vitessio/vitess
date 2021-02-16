// Recording rules can be used to continually evaluate queries and
// store their results as new timeseries. This is commonly used to
// calculate expensive aggregates prior to querying them. You can have
// any number of rules, split across any number of files.
//
// Reference: https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/
local config = import '../config.libsonnet';

{
  prometheusRules+:: {
    groups: [
      {
        name: 'vitess_mixin_1',
        rules: [
          {
            record: 'vitess_mixin:vttablet_error_byregion:irate1m',
            expr: 'sum by(region)(irate(vttablet_errors[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_2',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_counts_byregion:irate1m',
            expr: 'sum by(region)(irate(vttablet_query_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_3',
        rules: [
          {
            record: 'vitess_mixin:mysql_global_status_queries_byregion:irate1m',
            expr: 'sum by(region)(irate(mysql_global_status_queries[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_4',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_bucket_byregion:irate1m',
            expr: 'sum by(le,region)(irate(vtgate_api_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_5',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_bucket_byregion_keyspace:irate1m',
            expr: 'sum by(le,region,keyspace)(irate(vtgate_api_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_6',
        rules: [
          {
            record: 'vitess_mixin:vtgate_queries_processed_by_region_and_keyspace:irate1m',
            expr: 'sum by (region, keyspace) (irate(vtgate_queries_processed_by_table{plan!="Rollback"}[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_7',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_byregion:irate1m',
            expr: 'sum by(region)(irate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_8',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_count_byregion:irate1m',
            expr: 'sum by(region)(irate(vtgate_api_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_9',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_byregion_keyspace:irate1m',
            expr: 'sum by(region,keyspace)(irate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_10',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_count_byregion_keyspace:irate1m',
            expr: 'sum by(region,keyspace)(irate(vtgate_api_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_11',
        rules: [
          {
            record: 'vitess_mixin:mysql_global_status_slow_queries_byregion_keyspace_shard:irate1m',
            expr: 'sum by(region,keyspace,shard)(irate(mysql_global_status_slow_queries[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_12',
        rules: [
          {
            record: 'vitess_mixin:vttablet_kills_byregion_keyspace_shard:irate1m',
            expr: 'sum by (region,keyspace,shard)(irate(vttablet_kills[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_13',
        rules: [
          {
            record: 'vitess_mixin:vtgate_vttablet_call_error_count_byinstance:rate1m',
            expr: 'sum by(instance)(rate(vtgate_vttablet_call_error_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_14',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_byregion_db_type:irate1m',
            expr: 'sum by(region,db_type)(irate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_15',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_count_byregion_db_type:irate1m',
            expr: 'sum by(region,db_type)(irate(vtgate_api_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_16',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_bucket_byregion_db_type:irate1m',
            expr: 'sum by(le,region,db_type)(irate(vtgate_api_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_17',
        rules: [
          {
            record: 'vitess_mixin:vtgate_queries_processed_byregion_keyspace_table:irate1m',
            expr: 'sum(irate(vtgate_queries_processed_by_table[1m])) by (region,keyspace,table)',
          },
        ],
      },
      {
        name: 'vitess_mixin_18',
        rules: [
          {
            record: 'vitess_mixin:vtgate_queries_processed_byregion_keyspace_table_plan:irate1m',
            expr: 'sum(irate(vtgate_queries_processed_by_table[1m])) by (region,keyspace,table,plan)',
          },
        ],
      },
      {
        name: 'vitess_mixin_19',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_byregion_operation:irate1m',
            expr: 'sum by(region,operation)(irate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_20',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_byregion_code:irate1m',
            expr: 'sum by(region,code)(irate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_21',
        rules: [
          {
            record: 'vitess_mixin:process_start_time_seconds_byregion_job:sum5m',
            expr: 'sum by (region,job) (changes (process_start_time_seconds[5m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_22',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_counts_byregion_keyspace_table:irate1m',
            expr: 'sum by(region,keyspace,table)(irate(vttablet_query_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_23',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_counts_byregion_keyspace_table_plan:irate1m',
            expr: 'sum by(region,keyspace,table,plan)(irate(vttablet_query_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_24',
        rules: [
          {
            record: 'vitess_mixin:vttablet_errors_byregion_keyspace:irate1m',
            expr: 'sum by(region,keyspace)(irate(vttablet_errors[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_25',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_counts_byregion_keyspace:irate1m',
            expr: 'sum by(region,keyspace)(irate(vttablet_query_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_26',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table:irate1m',
            expr: 'sum by(region,keyspace,table)(irate(vttablet_query_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_27',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table_plan:irate1m',
            expr: 'sum by(region,keyspace,table,plan)(irate(vttablet_query_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_28',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_error_counts_byregion_keyspace_table_shard:irate1m',
            expr: 'sum by(region,keyspace,table,shard)(irate(vttablet_query_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_29',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_row_counts_byregion_keyspace_table:rate1m',
            expr: 'sum by(region,keyspace,table)(rate(vttablet_query_row_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_30',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_row_counts_byregion_keyspace_table_plan:rate1m',
            expr: 'sum by(region,keyspace,table,plan)(rate(vttablet_query_row_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_31',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_row_counts_byregion_keyspace_table_shard:rate1m',
            expr: 'sum by(region,keyspace,table,shard)(rate(vttablet_query_row_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_32',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_sum_byregion_keyspace:rate1m',
            expr: 'sum by(region,keyspace)(rate(vttablet_queries_sum[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_33',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_count_byregion_keyspace:rate1m',
            expr: 'sum by(region,keyspace)(rate(vttablet_queries_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_34',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_sum_byregion_keyspace_shard:rate1m',
            expr: 'sum by(region,keyspace,shard)(rate(vttablet_queries_sum[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_35',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_count_byregion_keyspace_shard:rate1m',
            expr: 'sum by(region,keyspace,shard)(rate(vttablet_queries_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_36',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_bucket_byregion_keyspace:rate1m',
            expr: 'sum by(region,keyspace,le)(rate(vttablet_queries_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_37',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_bucket_byregion_keyspace_shard:rate1m',
            expr: 'sum by(region,keyspace,shard,le)(rate(vttablet_queries_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_38',
        rules: [
          {
            record: 'vitess_mixin:vttablet_transactions_sum_byregion_keyspace:rate1m',
            expr: 'sum by(region,keyspace)(rate(vttablet_transactions_sum[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_39',
        rules: [
          {
            record: 'vitess_mixin:vttablet_transactions_count_byregion_keyspace:rate1m',
            expr: 'sum by(region,keyspace)(rate(vttablet_transactions_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_40',
        rules: [
          {
            record: 'vitess_mixin:vttablet_transactions_sum_byregion_keyspace_shard:rate1m',
            expr: 'sum by(region,keyspace,shard)(rate(vttablet_transactions_sum[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_41',
        rules: [
          {
            record: 'vitess_mixin:vttablet_transactions_count_byregion_keyspace_shard:rate1m',
            expr: 'sum by(region,keyspace,shard)(rate(vttablet_transactions_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_42',
        rules: [
          {
            record: 'vitess_mixin:vttablet_transactions_bucket_byregion_keyspace:rate1m',
            expr: 'sum by(region,keyspace,le)(rate(vttablet_transactions_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_43',
        rules: [
          {
            record: 'vitess_mixin:vttablet_transactions_bucket_byregion_keyspace_shard:rate1m',
            expr: 'sum by(region,keyspace,shard,le)(rate(vttablet_transactions_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_44',
        rules: [
          {
            record: 'vitess_mixin:process_start_time_seconds_byregion_keyspace_shard_job:sum5m',
            expr: 'sum by (region,keyspace,shard,job) (changes (process_start_time_seconds[5m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_45',
        rules: [
          {
            record: 'vitess_mixin:process_start_time_seconds_byregion_instance_job:sum5m',
            expr: 'sum by (region,instance,job) (changes (process_start_time_seconds[5m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_46',
        rules: [
          {
            record: 'vitess_mixin:vttablet_kills:irate1m',
            expr: 'sum by(keyspace,shard,instance,region)(irate(vttablet_kills[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_47',
        rules: [
          {
            record: 'vitess_mixin:vttablet_errors:irate1m',
            expr: 'sum by(keyspace,shard,instance,error_code,region)(irate(vttablet_errors[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_48',
        rules: [
          {
            record: 'vitess_mixin:vtgate_queries_processed_by_table:irate1m',
            expr: 'sum by(keyspace, plan, table, region) (irate(vtgate_queries_processed_by_table{plan!="Rollback"}[1m]))',
          },
        ],
      },
    ],
  },
}
