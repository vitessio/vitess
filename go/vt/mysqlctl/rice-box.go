package mysqlctl

import (
	"time"

	"github.com/GeertJohan/go.rice/embedded"
)

func init() {

	// define files
	file2 := &embedded.EmbeddedFile{
		Filename:    "gomysql.pc.tmpl",
		FileModTime: time.Unix(1539121419, 0),

		Content: string("Name: GoMysql\nDescription: Flags for using mysql C client in go\n"),
	}
	file3 := &embedded.EmbeddedFile{
		Filename:    "init_db.sql",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is executed immediately after mysql_install_db,\n# to initialize a fresh data directory.\n\n###############################################################################\n# WARNING: This sql is *NOT* safe for production use,\n#          as it contains default well-known users and passwords.\n#          Care should be taken to change these users and passwords\n#          for production.\n###############################################################################\n\n###############################################################################\n# Equivalent of mysql_secure_installation\n###############################################################################\n\n# Changes during the init db should not make it to the binlog.\n# They could potentially create errant transactions on replicas.\nSET sql_log_bin = 0;\n# Remove anonymous users.\nDELETE FROM mysql.user WHERE User = '';\n\n# Disable remote root access (only allow UNIX socket).\nDELETE FROM mysql.user WHERE User = 'root' AND Host != 'localhost';\n\n# Remove test database.\nDROP DATABASE IF EXISTS test;\n\n###############################################################################\n# Vitess defaults\n###############################################################################\n\n# Vitess-internal database.\nCREATE DATABASE IF NOT EXISTS _vt;\n# Note that definitions of local_metadata and shard_metadata should be the same\n# as in production which is defined in go/vt/mysqlctl/metadata_tables.go.\nCREATE TABLE IF NOT EXISTS _vt.local_metadata (\n  name VARCHAR(255) NOT NULL,\n  value VARCHAR(255) NOT NULL,\n  db_name VARBINARY(255) NOT NULL,\n  PRIMARY KEY (db_name, name)\n  ) ENGINE=InnoDB;\nCREATE TABLE IF NOT EXISTS _vt.shard_metadata (\n  name VARCHAR(255) NOT NULL,\n  value MEDIUMBLOB NOT NULL,\n  db_name VARBINARY(255) NOT NULL,\n  PRIMARY KEY (db_name, name)\n  ) ENGINE=InnoDB;\n\n# Admin user with all privileges.\nCREATE USER 'vt_dba'@'localhost';\nGRANT ALL ON *.* TO 'vt_dba'@'localhost';\nGRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';\n\n# User for app traffic, with global read-write access.\nCREATE USER 'vt_app'@'localhost';\nGRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,\n  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,\n  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,\n  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER\n  ON *.* TO 'vt_app'@'localhost';\n\n# User for app debug traffic, with global read access.\nCREATE USER 'vt_appdebug'@'localhost';\nGRANT SELECT, SHOW DATABASES, PROCESS ON *.* TO 'vt_appdebug'@'localhost';\n\n# User for administrative operations that need to be executed as non-SUPER.\n# Same permissions as vt_app here.\nCREATE USER 'vt_allprivs'@'localhost';\nGRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,\n  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,\n  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,\n  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER\n  ON *.* TO 'vt_allprivs'@'localhost';\n\n# User for slave replication connections.\nCREATE USER 'vt_repl'@'%';\nGRANT REPLICATION SLAVE ON *.* TO 'vt_repl'@'%';\n\n# User for Vitess filtered replication (binlog player).\n# Same permissions as vt_app.\nCREATE USER 'vt_filtered'@'localhost';\nGRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,\n  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,\n  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,\n  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER\n  ON *.* TO 'vt_filtered'@'localhost';\n\n# User for general MySQL monitoring.\nCREATE USER 'vt_monitoring'@'localhost';\nGRANT SELECT, PROCESS, SUPER, REPLICATION CLIENT, RELOAD\n  ON *.* TO 'vt_monitoring'@'localhost';\nGRANT SELECT, UPDATE, DELETE, DROP\n  ON performance_schema.* TO 'vt_monitoring'@'localhost';\n\n# User for Orchestrator (https://github.com/openark/orchestrator).\nCREATE USER 'orc_client_user'@'%' IDENTIFIED BY 'orc_client_user_password';\nGRANT SUPER, PROCESS, REPLICATION SLAVE, RELOAD\n  ON *.* TO 'orc_client_user'@'%';\nGRANT SELECT\n  ON _vt.* TO 'orc_client_user'@'%';\n\nFLUSH PRIVILEGES;\n\nRESET SLAVE ALL;\nRESET MASTER;\n"),
	}
	file5 := &embedded.EmbeddedFile{
		Filename:    "mycnf/default-fast.cnf",
		FileModTime: time.Unix(1600988485, 0),

		Content: string("# This sets some unsafe settings specifically for \n# the test-suite which is currently MySQL 5.7 based\n# In future it should be renamed testsuite.cnf\n\ninnodb_buffer_pool_size = 32M\ninnodb_flush_log_at_trx_commit = 0\ninnodb_log_buffer_size = 1M\ninnodb_log_file_size = 5M\n\n# Native AIO tends to run into aio-max-nr limit during test startup.\ninnodb_use_native_aio = 0\n\nkey_buffer_size = 2M\nsync_binlog=0\ninnodb_doublewrite=0\n\n# These two settings are required for the testsuite to pass, \n# but enabling them does not spark joy. They should be removed\n# in the future. See:\n# https://github.com/vitessio/vitess/issues/5396\n\nsql_mode = STRICT_TRANS_TABLES\n\n# set a short heartbeat interval in order to detect failures quickly\nslave_net_timeout = 4\n"),
	}
	file6 := &embedded.EmbeddedFile{
		Filename:    "mycnf/default.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# Global configuration that is auto-included for all MySQL/MariaDB versions\n\ndatadir = {{.DataDir}}\ninnodb_data_home_dir = {{.InnodbDataHomeDir}}\ninnodb_log_group_home_dir = {{.InnodbLogGroupHomeDir}}\nlog-error = {{.ErrorLogPath}}\nlog-bin = {{.BinLogPath}}\nrelay-log = {{.RelayLogPath}}\nrelay-log-index =  {{.RelayLogIndexPath}}\npid-file = {{.PidFile}}\nport = {{.MysqlPort}}\n\n# all db instances should start in read-only mode - once the db is started and\n# fully functional, we'll push it into read-write mode\nread-only\nserver-id = {{.ServerID}}\n\n# all db instances should skip the slave startup - that way we can do any\n# additional configuration (like enabling semi-sync) before we connect to\n# the master.\nskip_slave_start\nsocket = {{.SocketFile}}\ntmpdir = {{.TmpDir}}\n\nslow-query-log-file = {{.SlowLogPath}}\n\n# These are sensible defaults that apply to all MySQL/MariaDB versions\n\nlong_query_time = 2\nslow-query-log\nskip-name-resolve\nconnect_timeout = 30\ninnodb_lock_wait_timeout = 20\nmax_allowed_packet = 64M\nmax_connections = 500\n\n\n"),
	}
	file7 := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mariadb100.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MariaDB 10.0 is detected.\n\n# Semi-sync replication is required for automated unplanned failover\n# (when the master goes away). Here we just load the plugin so it's\n# available if desired, but it's disabled at startup.\n#\n# If the -enable_semi_sync flag is used, VTTablet will enable semi-sync\n# at the proper time when replication is set up, or when masters are\n# promoted or demoted.\nplugin-load = rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so\n\nslave_net_timeout = 60\n\n# MariaDB 10.0 is unstrict by default\nsql_mode = STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION\n\n# enable strict mode so it's safe to compare sequence numbers across different server IDs.\ngtid_strict_mode = 1\ninnodb_stats_persistent = 0\n\n# When semi-sync is enabled, don't allow fallback to async\n# if you get no ack, or have no slaves. This is necessary to\n# prevent alternate futures when doing a failover in response to\n# a master that becomes unresponsive.\nrpl_semi_sync_master_timeout = 1000000000000000000\nrpl_semi_sync_master_wait_no_slave = 1\n\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\nexpire_logs_days = 3\n\nsync_binlog = 1\nbinlog_format = ROW\nlog_slave_updates\nexpire_logs_days = 3\n\n# In MariaDB the default charset is latin1\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\n"),
	}
	file8 := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mariadb101.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MariaDB 10.1 is detected.\n\n# Semi-sync replication is required for automated unplanned failover\n# (when the master goes away). Here we just load the plugin so it's\n# available if desired, but it's disabled at startup.\n#\n# If the -enable_semi_sync flag is used, VTTablet will enable semi-sync\n# at the proper time when replication is set up, or when masters are\n# promoted or demoted.\nplugin-load = rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so\n\nslave_net_timeout = 60\n\n# MariaDB 10.1 default is only no-engine-substitution and no-auto-create-user\nsql_mode = STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,NO_AUTO_CREATE_USER\n\n# enable strict mode so it's safe to compare sequence numbers across different server IDs.\ngtid_strict_mode = 1\ninnodb_stats_persistent = 0\n\n# When semi-sync is enabled, don't allow fallback to async\n# if you get no ack, or have no slaves. This is necessary to\n# prevent alternate futures when doing a failover in response to\n# a master that becomes unresponsive.\nrpl_semi_sync_master_timeout = 1000000000000000000\nrpl_semi_sync_master_wait_no_slave = 1\n\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\nexpire_logs_days = 3\n\nsync_binlog = 1\nbinlog_format = ROW\nlog_slave_updates\nexpire_logs_days = 3\n\n# In MariaDB the default charset is latin1\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n"),
	}
	file9 := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mariadb102.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MariaDB 10.2 is detected.\n\n# Semi-sync replication is required for automated unplanned failover\n# (when the master goes away). Here we just load the plugin so it's\n# available if desired, but it's disabled at startup.\n#\n# If the -enable_semi_sync flag is used, VTTablet will enable semi-sync\n# at the proper time when replication is set up, or when masters are\n# promoted or demoted.\nplugin-load = rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so\n\n# enable strict mode so it's safe to compare sequence numbers across different server IDs.\ngtid_strict_mode = 1\ninnodb_stats_persistent = 0\n\n# When semi-sync is enabled, don't allow fallback to async\n# if you get no ack, or have no slaves. This is necessary to\n# prevent alternate futures when doing a failover in response to\n# a master that becomes unresponsive.\nrpl_semi_sync_master_timeout = 1000000000000000000\nrpl_semi_sync_master_wait_no_slave = 1\n\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\nexpire_logs_days = 3\n\nsync_binlog = 1\nbinlog_format = ROW\nlog_slave_updates\nexpire_logs_days = 3\n\n# In MariaDB the default charset is latin1\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n"),
	}
	filea := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mariadb103.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MariaDB 10.3 is detected.\n\n# enable strict mode so it's safe to compare sequence numbers across different server IDs.\ngtid_strict_mode = 1\ninnodb_stats_persistent = 0\n\n# When semi-sync is enabled, don't allow fallback to async\n# if you get no ack, or have no slaves. This is necessary to\n# prevent alternate futures when doing a failover in response to\n# a master that becomes unresponsive.\nrpl_semi_sync_master_timeout = 1000000000000000000\nrpl_semi_sync_master_wait_no_slave = 1\n\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\nexpire_logs_days = 3\n\nsync_binlog = 1\nbinlog_format = ROW\nlog_slave_updates\nexpire_logs_days = 3\n\n# In MariaDB the default charset is latin1\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\n\n"),
	}
	fileb := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mariadb104.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MariaDB 10.4 is detected.\n\n# enable strict mode so it's safe to compare sequence numbers across different server IDs.\ngtid_strict_mode = 1\ninnodb_stats_persistent = 0\n\n# When semi-sync is enabled, don't allow fallback to async\n# if you get no ack, or have no slaves. This is necessary to\n# prevent alternate futures when doing a failover in response to\n# a master that becomes unresponsive.\nrpl_semi_sync_master_timeout = 1000000000000000000\nrpl_semi_sync_master_wait_no_slave = 1\n\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\nexpire_logs_days = 3\n\nsync_binlog = 1\nbinlog_format = ROW\nlog_slave_updates\nexpire_logs_days = 3\n\n# In MariaDB the default charset is latin1\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\n\n"),
	}
	filec := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mysql56.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MySQL 5.6 is detected.\n\n# MySQL 5.6 does not enable the binary log by default, and \n# the default for sync_binlog is unsafe. The format is TABLE, and\n# info repositories also default to file.\n\nsync_binlog = 1\ngtid_mode = ON\nbinlog_format = ROW\nlog_slave_updates\nenforce_gtid_consistency\nexpire_logs_days = 3\nmaster_info_repository = TABLE\nrelay_log_info_repository = TABLE\nrelay_log_purge = 1\nrelay_log_recovery = 1\nslave_net_timeout = 60\n\n# In MySQL 5.6 the default charset is latin1\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\n# MySQL 5.6 is unstrict by default\nsql_mode = STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION\n\n# Semi-sync replication is required for automated unplanned failover\n# (when the master goes away). Here we just load the plugin so it's\n# available if desired, but it's disabled at startup.\n#\n# If the -enable_semi_sync flag is used, VTTablet will enable semi-sync\n# at the proper time when replication is set up, or when masters are\n# promoted or demoted.\nplugin-load = rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so\n\n# When semi-sync is enabled, don't allow fallback to async\n# if you get no ack, or have no slaves. This is necessary to\n# prevent alternate futures when doing a failover in response to\n# a master that becomes unresponsive.\nrpl_semi_sync_master_timeout = 1000000000000000000\nrpl_semi_sync_master_wait_no_slave = 1\n\n"),
	}
	filed := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mysql57.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MySQL 5.7 is detected.\n\n# MySQL 5.7 does not enable the binary log by default, and \n# info repositories default to file\n\ngtid_mode = ON\nlog_slave_updates\nenforce_gtid_consistency\nexpire_logs_days = 3\nmaster_info_repository = TABLE\nrelay_log_info_repository = TABLE\nrelay_log_purge = 1\nrelay_log_recovery = 1\n\n# In MySQL 5.7 the default charset is latin1\n\ncharacter_set_server = utf8\ncollation_server = utf8_general_ci\n\n# Semi-sync replication is required for automated unplanned failover\n# (when the master goes away). Here we just load the plugin so it's\n# available if desired, but it's disabled at startup.\n#\n# If the -enable_semi_sync flag is used, VTTablet will enable semi-sync\n# at the proper time when replication is set up, or when masters are\n# promoted or demoted.\nplugin-load = rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so\n\n# When semi-sync is enabled, don't allow fallback to async\n# if you get no ack, or have no slaves. This is necessary to\n# prevent alternate futures when doing a failover in response to\n# a master that becomes unresponsive.\nrpl_semi_sync_master_timeout = 1000000000000000000\nrpl_semi_sync_master_wait_no_slave = 1\n\n"),
	}
	filee := &embedded.EmbeddedFile{
		Filename:    "mycnf/master_mysql80.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is auto-included when MySQL 8.0 is detected.\n\n# MySQL 8.0 enables binlog by default with sync_binlog and TABLE info repositories\n# It does not enable GTIDs or enforced GTID consistency\n\ngtid_mode = ON\nenforce_gtid_consistency\nrelay_log_recovery = 1\nbinlog_expire_logs_seconds = 259200\n\n# disable mysqlx\nmysqlx = 0\n\n# 8.0 changes the default auth-plugin to caching_sha2_password\ndefault_authentication_plugin = mysql_native_password\n\n# Semi-sync replication is required for automated unplanned failover\n# (when the master goes away). Here we just load the plugin so it's\n# available if desired, but it's disabled at startup.\n#\n# If the -enable_semi_sync flag is used, VTTablet will enable semi-sync\n# at the proper time when replication is set up, or when masters are\n# promoted or demoted.\nplugin-load = rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so\n\n# MySQL 8.0 will not load plugins during --initialize\n# which makes these options unknown. Prefixing with --loose\n# tells the server it's fine if they are not understood.\nloose_rpl_semi_sync_master_timeout = 1000000000000000000\nloose_rpl_semi_sync_master_wait_no_slave = 1\n\n"),
	}
	filef := &embedded.EmbeddedFile{
		Filename:    "mycnf/sbr.cnf",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("# This file is used to allow legacy tests to pass\n# In theory it should not be required\nbinlog_format=statement\n"),
	}
	fileh := &embedded.EmbeddedFile{
		Filename:    "orchestrator/default.json",
		FileModTime: time.Unix(1600387438, 0),

		Content: string("{\n  \"Debug\": true,\n  \"MySQLTopologyUser\": \"orc_client_user\",\n  \"MySQLTopologyPassword\": \"orc_client_user_password\",\n  \"MySQLReplicaUser\": \"vt_repl\",\n  \"MySQLReplicaPassword\": \"\",\n  \"RecoveryPeriodBlockSeconds\": 5\n}\n"),
	}
	filej := &embedded.EmbeddedFile{
		Filename:    "tablet/default.yaml",
		FileModTime: time.Unix(1599694847, 0),

		Content: string("tabletID: zone-1234\n\ninit:\n  dbName:            # init_db_name_override\n  keyspace:          # init_keyspace\n  shard:             # init_shard\n  tabletType:        # init_tablet_type\n  timeoutSeconds: 60 # init_timeout\n\ndb:\n  socket:     # db_socket\n  host:       # db_host\n  port: 0     # db_port\n  charSet:    # db_charset\n  flags: 0    # db_flags\n  flavor:     # db_flavor\n  sslCa:      # db_ssl_ca\n  sslCaPath:  # db_ssl_ca_path\n  sslCert:    # db_ssl_cert\n  sslKey:     # db_ssl_key\n  serverName: # db_server_name\n  connectTimeoutMilliseconds: 0 # db_connect_timeout_ms\n  app:\n    user: vt_app      # db_app_user\n    password:         # db_app_password\n    useSsl: true      # db_app_use_ssl\n    preferTcp: false\n  dba:\n    user: vt_dba      # db_dba_user\n    password:         # db_dba_password\n    useSsl: true      # db_dba_use_ssl\n    preferTcp: false\n  filtered:\n    user: vt_filtered # db_filtered_user\n    password:         # db_filtered_password\n    useSsl: true      # db_filtered_use_ssl\n    preferTcp: false\n  repl:\n    user: vt_repl     # db_repl_user\n    password:         # db_repl_password\n    useSsl: true      # db_repl_use_ssl\n    preferTcp: false\n  appdebug:\n    user: vt_appdebug # db_appdebug_user\n    password:         # db_appdebug_password\n    useSsl: true      # db_appdebug_use_ssl\n    preferTcp: false\n  allprivs:\n    user: vt_allprivs # db_allprivs_user\n    password:         # db_allprivs_password\n    useSsl: true      # db_allprivs_use_ssl\n    preferTcp: false\n\noltpReadPool:\n  size: 16                 # queryserver-config-pool-size\n  timeoutSeconds: 0        # queryserver-config-query-pool-timeout\n  idleTimeoutSeconds: 1800 # queryserver-config-idle-timeout\n  prefillParallelism: 0    # queryserver-config-pool-prefill-parallelism\n  maxWaiters: 50000        # queryserver-config-query-pool-waiter-cap\n\nolapReadPool:\n  size: 200                # queryserver-config-stream-pool-size\n  timeoutSeconds: 0        # queryserver-config-query-pool-timeout\n  idleTimeoutSeconds: 1800 # queryserver-config-idle-timeout\n  prefillParallelism: 0    # queryserver-config-stream-pool-prefill-parallelism\n  maxWaiters: 0\n\ntxPool:\n  size: 20                 # queryserver-config-transaction-cap\n  timeoutSeconds: 1        # queryserver-config-txpool-timeout\n  idleTimeoutSeconds: 1800 # queryserver-config-idle-timeout\n  prefillParallelism: 0    # queryserver-config-transaction-prefill-parallelism\n  maxWaiters: 50000        # queryserver-config-txpool-waiter-cap\n\noltp:\n  queryTimeoutSeconds: 30 # queryserver-config-query-timeout\n  txTimeoutSeconds: 30    # queryserver-config-transaction-timeout\n  maxRows: 10000          # queryserver-config-max-result-size\n  warnRows: 0             # queryserver-config-warn-result-size\n\nhealthcheck:\n  intervalSeconds: 20             # health_check_interval\n  degradedThresholdSeconds: 30    # degraded_threshold\n  unhealthyThresholdSeconds: 7200 # unhealthy_threshold\n\ngracePeriods:\n  transactionShutdownSeconds: 0 # transaction_shutdown_grace_period\n  transitionSeconds: 0          # serving_state_grace_period\n\nreplicationTracker:\n  mode: disable                    # enable_replication_reporter\n  heartbeatIntervalMilliseconds: 0 # heartbeat_enable, heartbeat_interval\n\nhotRowProtection:\n  mode: disable|dryRun|enable # enable_hot_row_protection, enable_hot_row_protection_dry_run\n  # Recommended value: same as txPool.size.\n  maxQueueSize: 20            # hot_row_protection_max_queue_size\n  maxGlobalQueueSize: 1000    # hot_row_protection_max_global_queue_size\n  maxConcurrency: 5           # hot_row_protection_concurrent_transactions\n\nconsolidator: enable|disable|notOnMaster # enable-consolidator, enable-consolidator-replicas\npassthroughDML: false                    # queryserver-config-passthrough-dmls\nstreamBufferSize: 32768                  # queryserver-config-stream-buffer-size\nqueryCacheSize: 5000                     # queryserver-config-query-cache-size\nschemaReloadIntervalSeconds: 1800        # queryserver-config-schema-reload-time\nwatchReplication: false                  # watch_replication_stream\nterseErrors: false                       # queryserver-config-terse-errors\nmessagePostponeParallelism: 4            # queryserver-config-message-postpone-cap\ncacheResultFields: true                  # enable-query-plan-field-caching\n\n\n# The following flags are currently not supported.\n# enforce_strict_trans_tables\n# queryserver-config-strict-table-acl\n# queryserver-config-enable-table-acl-dry-run\n# queryserver-config-acl-exempt-acl\n# enable-tx-throttler\n# tx-throttler-config\n# tx-throttler-healthcheck-cells\n# enable_transaction_limit\n# enable_transaction_limit_dry_run\n# transaction_limit_per_user\n# transaction_limit_by_username\n# transaction_limit_by_principal\n# transaction_limit_by_component\n# transaction_limit_by_subcomponent\n"),
	}
	filek := &embedded.EmbeddedFile{
		Filename:    "zk-client-dev.json",
		FileModTime: time.Unix(1539121419, 0),

		Content: string("{\n  \"local\": \"localhost:3863\",\n  \"global\": \"localhost:3963\"\n}\n"),
	}
	filem := &embedded.EmbeddedFile{
		Filename:    "zkcfg/zoo.cfg",
		FileModTime: time.Unix(1539121419, 0),

		Content: string("tickTime=2000\ndataDir={{.DataDir}}\nclientPort={{.ClientPort}}\ninitLimit=5\nsyncLimit=2\nmaxClientCnxns=0\n{{range .Servers}}\nserver.{{.ServerId}}={{.Hostname}}:{{.LeaderPort}}:{{.ElectionPort}}\n{{end}}\n"),
	}

	// define dirs
	dir1 := &embedded.EmbeddedDir{
		Filename:   "",
		DirModTime: time.Unix(1599765277, 0),
		ChildFiles: []*embedded.EmbeddedFile{
			file2, // "gomysql.pc.tmpl"
			file3, // "init_db.sql"
			filek, // "zk-client-dev.json"

		},
	}
	dir4 := &embedded.EmbeddedDir{
		Filename:   "mycnf",
		DirModTime: time.Unix(1600988485, 0),
		ChildFiles: []*embedded.EmbeddedFile{
			file5, // "mycnf/default-fast.cnf"
			file6, // "mycnf/default.cnf"
			file7, // "mycnf/master_mariadb100.cnf"
			file8, // "mycnf/master_mariadb101.cnf"
			file9, // "mycnf/master_mariadb102.cnf"
			filea, // "mycnf/master_mariadb103.cnf"
			fileb, // "mycnf/master_mariadb104.cnf"
			filec, // "mycnf/master_mysql56.cnf"
			filed, // "mycnf/master_mysql57.cnf"
			filee, // "mycnf/master_mysql80.cnf"
			filef, // "mycnf/sbr.cnf"

		},
	}
	dirg := &embedded.EmbeddedDir{
		Filename:   "orchestrator",
		DirModTime: time.Unix(1600387438, 0),
		ChildFiles: []*embedded.EmbeddedFile{
			fileh, // "orchestrator/default.json"

		},
	}
	diri := &embedded.EmbeddedDir{
		Filename:   "tablet",
		DirModTime: time.Unix(1599694847, 0),
		ChildFiles: []*embedded.EmbeddedFile{
			filej, // "tablet/default.yaml"

		},
	}
	dirl := &embedded.EmbeddedDir{
		Filename:   "zkcfg",
		DirModTime: time.Unix(1539121419, 0),
		ChildFiles: []*embedded.EmbeddedFile{
			filem, // "zkcfg/zoo.cfg"

		},
	}

	// link ChildDirs
	dir1.ChildDirs = []*embedded.EmbeddedDir{
		dir4, // "mycnf"
		dirg, // "orchestrator"
		diri, // "tablet"
		dirl, // "zkcfg"

	}
	dir4.ChildDirs = []*embedded.EmbeddedDir{}
	dirg.ChildDirs = []*embedded.EmbeddedDir{}
	diri.ChildDirs = []*embedded.EmbeddedDir{}
	dirl.ChildDirs = []*embedded.EmbeddedDir{}

	// register embeddedBox
	embedded.RegisterEmbeddedBox(`../../../config`, &embedded.EmbeddedBox{
		Name: `../../../config`,
		Time: time.Unix(1599765277, 0),
		Dirs: map[string]*embedded.EmbeddedDir{
			"":             dir1,
			"mycnf":        dir4,
			"orchestrator": dirg,
			"tablet":       diri,
			"zkcfg":        dirl,
		},
		Files: map[string]*embedded.EmbeddedFile{
			"gomysql.pc.tmpl":             file2,
			"init_db.sql":                 file3,
			"mycnf/default-fast.cnf":      file5,
			"mycnf/default.cnf":           file6,
			"mycnf/master_mariadb100.cnf": file7,
			"mycnf/master_mariadb101.cnf": file8,
			"mycnf/master_mariadb102.cnf": file9,
			"mycnf/master_mariadb103.cnf": filea,
			"mycnf/master_mariadb104.cnf": fileb,
			"mycnf/master_mysql56.cnf":    filec,
			"mycnf/master_mysql57.cnf":    filed,
			"mycnf/master_mysql80.cnf":    filee,
			"mycnf/sbr.cnf":               filef,
			"orchestrator/default.json":   fileh,
			"tablet/default.yaml":         filej,
			"zk-client-dev.json":          filek,
			"zkcfg/zoo.cfg":               filem,
		},
	})
}
