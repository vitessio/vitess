VTTablet can be configured to control mysql at many levels. At the level with the most control, vttablet can perform backups and restores, respond to reparenting commands coming through vtctld, automatically fix replication, and enforce semi-sync settings.

At the level with the least control, vttablet just sends the application’s queries to mysql. The level of desired control is achieved through various command line arguments, explained below.

## Managed MySQL

In the mode with the highest control, VTTablet can take backups. It can also automatically restore from an existing backup to prime a new replica. For this mode, vttablet needs to run on the same host as mysql, and must be given access to mysql's my.cnf file. Additionally, the flags must not contain any connectivity parameters like `-db_host` or `-db_socket`; VTTablet will fetch the socket information from my.cnf and use that to connect to the local mysql.

It will also load other information from the my.cnf, like the location of data files, etc. When it receives a request to take a backup, it will shut down mysql, copy the mysql data files to the backup store, and restart mysql.

The my.cnf file can be specified the following ways:

* Implicit: If mysql was initialized by the `mysqlctl` tool, then vttablet can find it based on just the `-tablet-path`. The default location for this file is `$VTDATAROOT/vt_<tablet-path>/my.cnf`.
* `-mycnf-file`: This option can be used if the file is not present in the default location.
* `-my_cnf_server_id` and other flags: You can specify all values of the my.cnf file from the command line, and vttablet will behave as it read this information from a physical file.

Specifying a `-db_host` or a `-db_socket` parameter will cause vttablet to skip the loading of the my.cnf file, and will disable its ability to perform backups or restores.

### -restore\_from\_backup

The default value for this flag is false. If set to true, and the my.cnf file was successfully loaded, then vttablet can perform automatic restores as follows:

If started against a mysql instance that has no data files, it will search the list of backups for the latest one, and initiate a restore. After this, it will point the mysql to the current master and wait for replication to catch up. Once replication is caught up to the specified tolerance limit, it will advertise itself as serving. This will cause the vtgates to add it to the list of healthy tablets to serve queries from.

If this flag is true, but my.cnf was not loaded, then vttablet will fatally exit with an error message.

You can additionally control the level of concurrency for a restore with the `-restore_concurrency` flag. This is typically useful in cloud environments to prevent the restore process from becoming a 'noisy' neighbor by consuming all available disk IOPS.

## Unmanaged or remote MySQL

You can start a vttablet against a remote mysql by simply specifying the connection parameters `-db_host` and `-db_port` on the command line. In this mode, backup and restore operations will be disabled. If you start vttablet against a local mysql, you can specify a `-db_socket` instead, which will still make vttablet treat mysql as if it was remote.

Specifically, the absence of a my.cnf file indicates to vttablet that it's connecting to a remote MySQL.

## Partially managed MySQL

Even if a MySQL is remote, you can still make vttablet perform some management functions. They are as follows:

* `-disable_active_reparents`: If this flag is set, then any reparent or slave commands will not be allowed. These are InitShardMaster, PlannedReparent, PlannedReparent, EmergencyReparent, and ReparentTablet. In this mode, you should use the TabletExternallyReparented command to inform vitess of the current master.
* `-master_connect_retry`: This value is give to mysql when it connects a slave to the master as the retry duration parameter.
* `-enable_replication_reporter`: If this flag is set, then vttablet will transmit replica lag related information to the vtgates, which will allow it to balance load better. Additionally, enabling this will also cause vttablet to restart replication if it was stopped. However, it will do this only if -disable_active_reparents was not turned on.
* `-enable_semi_sync`: This option will automatically enable semi-sync on new replicas as well as on any tablet that transitions into a replica type. This includes the demotion of a master to a replica.
* `-heatbeat_enable` and `-heartbeat_interval_duration`: cause vttablet to write heartbeats to the sidecar database. This information is also used by the replication reporter to assess replica lag.

## Typical vttablet command line flags

### Minimal vttablet to enable query serving
```
$TOPOLOGY_FLAGS
-tablet-path $alias
-init_keyspace $keyspace
-init_shard $shard
-init_tablet_type $tablet_type
-port $port
-grpc_port $grpc_port
-service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream'
```

$alias needs to be of the form: `<cell>-id`, and the cell should match one of the local cells that was created in the topology. The id can be left padded with zeroes: `cell-100` and `cell-000000100` are synonymous.
  
Example TOPOLOGY\_FLAGS for a lockserver like zookeeper:

`-topo_implementation zk2 -topo_global_server_address localhost:21811,localhost:21812,localhost:21813 -topo_global_root /vitess/global`

### Additional parameters to enable cluster management
```
-enable_semi_sync
-enable_replication_reporter
-backup_storage_implementation file
-file_backup_storage_root $BACKUP_MOUNT
-restore_from_backup
-vtctld_addr http://$hostname:$vtctld_web_port/
```

### Additional parameters for running in prod

```
-queryserver-config-pool-size 24
-queryserver-config-stream-pool-size 24
-queryserver-config-transaction-cap 300
```

More tuning parameters are available, but the above overrides are definitely needed for serving reasonable production traffic.

### Connecting vttablet to an already running mysql

```
-db_host $MYSQL_HOST
-db_port $MYSQL_PORT
-db_app_user $USER
-db_app_password $PASSWORD
```

### Additional user credentials that need to be supplied for performing various operations

```
-db_allprivs_user
-db_allprivs_password
-db_appdebug_user
-db_appdebug_password
-db_dba_user
-db_dba_password
-db_filtered_user
-db_filtered_password
```

Other flags exist for finer control.
