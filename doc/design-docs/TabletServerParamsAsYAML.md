# VTTablet YAML configuration

## Background
This issue is an expansion of #5791, and covers the details of how we’ll use yaml to implement a hierarchy of parameters for initializing and customizing TabletServer components within a process.

Using the yaml approach, we intend to solve the following problems:

* Multiple tablet servers need to exist within a process.
* Badly named command line parameters: The new yaml specifications will have simpler and easier to remember parameter names.
* Sections for improved readability.
* Unreasonable default values: The new specs will introduce a simplified approach that will save the user from tweaking too many variables without knowing their consequences.
* Repetition: If multiple TabletServers have to share the same parameter values, they should not be repeated.
* Backward compatibility: The system must be backward compatible.

## Proposed Approach
We will introduce two new command-line options that will be specified like this:
```
-defaults=defaults.yaml -tablets=tablets.yaml
```

The defaults option allows the user to specify the default settings for all tablets. The “tablets” option allows the user to specify tablet specific options.

We’ll provide predefined “defaults” files that people can use as starting points. We’ll start with something simple like small.yaml, medium.yaml, and large.yaml.

### Protobuf
We explored the use of protobufs for converting to and from yaml. However, the protobuf JSON implementation, which was required to correctly parse enums, was unable to preserve the original values for sub-objects. This was a showstopper for making defaults work.

The existing `tabletenv.TabletConfig` data structure will be converted into a struct with the new names and appropriate JSON tags.

## Detailed specification
The following section lists all the properties that can be specified in a yaml file along with the existing defaults.

During implementation, we’ll need to decide between creating a universal protobuf that represents this structure vs converting the original one into something more usable.

Convention used: same as the one followed by kubernetes, and we’ll be making use of https://github.com/kubernetes-sigs/yaml for the implementation.

Note that certain properties (like tabletID) are only applicable to tablet-specific files.

```
tabletID: zone-1234

init:
  dbName:            # init_db_name_override
  keyspace:          # init_keyspace
  shard:             # init_shard
  tabletType:        # init_tablet_type
  timeoutSeconds: 60 # init_timeout

db:
  socket:     # db_socket
  host:       # db_host
  port: 0     # db_port
  charSet:    # db_charset
  flags: 0    # db_flags
  flavor:     # db_flavor
  sslCa:      # db_ssl_ca
  sslCaPath:  # db_ssl_ca_path
  sslCert:    # db_ssl_cert
  sslKey:     # db_ssl_key
  serverName: # db_server_name
  connectTimeoutMilliseconds: 0 # db_connect_timeout_ms
  app:
    user: vt_app      # db_app_user
    password:         # db_app_password
    useSsl: true      # db_app_use_ssl
    preferSocket: true
  dba:
    user: vt_dba      # db_dba_user
    password:         # db_dba_password
    useSsl: true      # db_dba_use_ssl
    preferSocket: true
  filtered:
    user: vt_filtered # db_filtered_user
    password:         # db_filtered_password
    useSsl: true      # db_filtered_use_ssl
    preferSocket: true
  repl:
    user: vt_repl     # db_repl_user
    password:         # db_repl_password
    useSsl: true      # db_repl_use_ssl
    preferSocket: true
  appdebug:
    user: vt_appdebug # db_appdebug_user
    password:         # db_appdebug_password
    useSsl: true      # db_appdebug_use_ssl
    preferSocket: true
  allprivs:
    user: vt_allprivs # db_allprivs_user
    password:         # db_allprivs_password
    useSsl: true      # db_allprivs_use_ssl
    preferSocket: true

oltpReadPool:
  size: 16                 # queryserver-config-pool-size
  timeoutSeconds: 0        # queryserver-config-query-pool-timeout
  idleTimeoutSeconds: 1800 # queryserver-config-idle-timeout
  prefillParallelism: 0    # queryserver-config-pool-prefill-parallelism
  maxWaiters: 50000        # queryserver-config-query-pool-waiter-cap

olapReadPool:
  size: 200                # queryserver-config-stream-pool-size
  timeoutSeconds: 0        # queryserver-config-query-pool-timeout
  idleTimeoutSeconds: 1800 # queryserver-config-idle-timeout
  prefillParallelism: 0    # queryserver-config-stream-pool-prefill-parallelism
  maxWaiters: 0

txPool:
  size: 20                 # queryserver-config-transaction-cap
  timeoutSeconds: 1        # queryserver-config-txpool-timeout
  idleTimeoutSeconds: 1800 # queryserver-config-idle-timeout
  prefillParallelism: 0    # queryserver-config-transaction-prefill-parallelism
  maxWaiters: 50000        # queryserver-config-txpool-waiter-cap

oltp:
  queryTimeoutSeconds: 30 # queryserver-config-query-timeout
  txTimeoutSeconds: 30    # queryserver-config-transaction-timeout
  maxRows: 10000          # queryserver-config-max-result-size
  warnRows: 0             # queryserver-config-warn-result-size

hotRowProtection:
  mode: disable|dryRun|enable # enable_hot_row_protection, enable_hot_row_protection_dry_run
  # Default value is same as txPool.size.
  maxQueueSize: 20            # hot_row_protection_max_queue_size
  maxGlobalQueueSize: 1000    # hot_row_protection_max_global_queue_size
  maxConcurrency: 5           # hot_row_protection_concurrent_transactions

consolidator: enable|disable|notOnPrimary # enable-consolidator, enable-consolidator-replicas
heartbeatIntervalMilliseconds: 0         # heartbeat_enable, heartbeat_interval
shutdownGracePeriodSeconds: 0            # transaction_shutdown_grace_period
passthroughDML: false                    # queryserver-config-passthrough-dmls
streamBufferSize: 32768                  # queryserver-config-stream-buffer-size
queryCacheSize: 5000                     # queryserver-config-query-cache-size
schemaReloadIntervalSeconds: 1800        # queryserver-config-schema-reload-time
watchReplication: false                  # watch_replication_stream
terseErrors: false                       # queryserver-config-terse-errors
messagePostponeParallelism: 4            # queryserver-config-message-postpone-cap
sanitizeLogMessages: false               # sanitize_log_messages


# The following flags are currently not supported.
# enforce_strict_trans_tables
# queryserver-config-strict-table-acl
# queryserver-config-enable-table-acl-dry-run
# queryserver-config-acl-exempt-acl
# enable-tx-throttler
# tx-throttler-config
# tx-throttler-healthcheck-cells
# tx-throttler-tablet-types
# enable_transaction_limit
# enable_transaction_limit_dry_run
# transaction_limit_per_user
# transaction_limit_by_username
# transaction_limit_by_principal
# transaction_limit_by_component
# transaction_limit_by_subcomponent
```

There are also other global parameters. VTTablet has a total of 405 flags. We may have to later move some of them into these yamls.

## Implementation
We'll use the unified tabletenv.TabletConfig data structure to load defaults as well as tablet-specific values. The following changes will be made:

* TabletConfig will be changed to match the above specifications.
* The existing flags will be changed to update the values in a global TabletConfig.
* In case of type mismatch (like time.Duration vs a "Seconds" variable), an extra step will be performed after parsing to convert the flag variables into TabletConfig members.
* The code will be changed to honor the new TabletConfig members.
* After the flag.Parse step, a copy of the global Config will be used as input to load the defaults file. This means that any command line flags that are not overridden in the yaml files will be preserved. This behavior is needed to support backward compatibility in case we decide to move more flags into the yaml.
* For each tablet entry, we create a copy of the result TabletConfig and read the tablet yaml into those, which will set the tablet-specific values. This will then be used to instantiate a TabletServer.

The exact format of the tablets.yaml file is not fully finalized. Our options are to allow a list of files where each is for a single tablet, or, to require only one file containing a dictionary of tablet-specific overrides.
