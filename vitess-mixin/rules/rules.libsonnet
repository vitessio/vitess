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
            record: 'vitess_mixin:vttablet_errors:rate1m',
            expr: 'sum (rate(vttablet_errors[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_2',
        rules: [
          {
            record: 'vitess_mixin:vttablet_query_counts:rate1m',
            expr: 'sum (rate(vttablet_query_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_3',
        rules: [
          {
            record: 'vitess_mixin:mysql_global_status_queries:rate1m',
            expr: 'sum (rate(mysql_global_status_queries[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_4',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_bucket:rate1m',
            expr: 'sum by(le)(rate(vtgate_api_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_5',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_bucket_by_keyspace:rate1m',
            expr: 'sum by(le,keyspace)(rate(vtgate_api_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_6',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts:rate1m',
            expr: 'sum (rate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_7',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_count:rate1m',
            expr: 'sum (rate(vtgate_api_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_8',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_by_keyspace:rate1m',
            expr: 'sum by(keyspace)(rate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_9',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_count_by_keyspace:rate1m',
            expr: 'sum by(keyspace)(rate(vtgate_api_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_10',
        rules: [
          {
            record: 'vitess_mixin:vttablet_kills:rate1m',
            expr: 'sum by (keyspace,shard)(rate(vttablet_kills[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_11',
        rules: [
          {
            record: 'vitess_mixin:vtgate_vttablet_call_error_count_byinstance:rate1m',
            expr: 'sum by(instance)(rate(vtgate_vttablet_call_error_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_12',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_by_db_type:rate1m',
            expr: 'sum by(db_type)(rate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_13',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_count_by_db_type:rate1m',
            expr: 'sum by(db_type)(rate(vtgate_api_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_14',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_bucket_by_db_type:rate1m',
            expr: 'sum by(le,db_type)(rate(vtgate_api_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_15',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_by_operation:rate1m',
            expr: 'sum by(operation)(rate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_16',
        rules: [
          {
            record: 'vitess_mixin:vtgate_api_error_counts_by_code:rate1m',
            expr: 'sum by(code)(rate(vtgate_api_error_counts[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_17',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_sum_by_keyspace_shard:rate1m',
            expr: 'sum by(keyspace,shard)(rate(vttablet_queries_sum[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_18',
        rules: [
          {
            record: 'vitess_mixin:vttablet_queries_count_by_keyspace_shard:rate1m',
            expr: 'sum by(keyspace,shard)(rate(vttablet_queries_count[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_19',
        rules: [
          {
            record: 'vitess_mixin:vttablet_transactions_bucket_by_keyspace_shard:rate1m',
            expr: 'sum by(keyspace,shard,le)(rate(vttablet_transactions_bucket[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_20',
        rules: [
          {
            record: 'vitess_mixin:process_start_time_seconds_by_instance_job:sum5m',
            expr: 'sum by (instance,job) (changes (process_start_time_seconds[5m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_21',
        rules: [
          {
            record: 'vitess_mixin:vttablet_kills_by_instance:rate1m',
            expr: 'sum by(instance)(rate(vttablet_kills[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_22',
        rules: [
          {
            record: 'vitess_mixin:vttablet_errors:rate1m',
            expr: 'sum by(keyspace,shard,instance,error_code)(rate(vttablet_errors[1m]))',
          },
        ],
      },
      {
        name: 'vitess_mixin_23',
        rules: [
          {
            record: 'vitess_mixin:vtgate_queries_processed_by_table:rate1m',
            expr: 'sum by(keyspace,plan,table) (rate(vtgate_queries_processed_by_table{plan!="Rollback"}[1m]))',
          },
        ],
      },
    ],
  },
}
