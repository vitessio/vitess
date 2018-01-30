###################################
# Orchestrator Config
###################################
{{- define "orchestrator-config" -}}
# set tuple values to more recognizable variables
{{- $orc := index . 0 -}}
{{- $namespace := index . 1 -}}

apiVersion: v1
kind: ConfigMap
metadata:
  name: orchestrator-cm
data: 
  orchestrator.conf.json: |-
    {
    "AccessTokenExpiryMinutes": 1440,
    "AccessTokenUseExpirySeconds": 60,
    "ActiveNodeExpireSeconds": 5,
    "AgentPollMinutes": 60,
    "AgentsServerPort": ":3001",
    "AgentSSLCAFile": "",
    "AgentSSLCertFile": "",
    "AgentSSLPrivateKeyFile": "",
    "AgentSSLSkipVerify": false,
    "AgentSSLValidOUs": [
    ],
    "AgentsUseMutualTLS": false,
    "AgentsUseSSL": false,
    "ApplyMySQLPromotionAfterMasterFailover": true,
    "AuditLogFile": "/tmp/orchestrator-audit.log",
    "AuditToSyslog": false,
    "AuthenticationMethod": "",
    "AuthUserHeader": "",
    "AutoPseudoGTID": false,
    "BackendDB": "sqlite",
    "BinlogEventsChunkSize": 10000,
    "CandidateInstanceExpireMinutes": 60,
    "ClusterNameToAlias": {
        "127.0.0.1": "test suite"
    },
    "CoMasterRecoveryMustPromoteOtherCoMaster": true,
    "DataCenterPattern": "[.]([^.]+)[.][^.]+[.]mydomain[.]com",
    "Debug": true,
    "DefaultInstancePort": 3306,
    "DefaultRaftPort": 10008,
    "DetachLostSlavesAfterMasterFailover": true,
    "DetectClusterAliasQuery": "SELECT value FROM _vt.local_metadata WHERE name='ClusterAlias'",
    "DetectClusterDomainQuery": "",
    "DetectInstanceAliasQuery": "SELECT value FROM _vt.local_metadata WHERE name='Alias'",
    "DetectPromotionRuleQuery": "SELECT value FROM _vt.local_metadata WHERE name='PromotionRule'",
    "DetectPseudoGTIDQuery": "",
    "DetectSemiSyncEnforcedQuery": "SELECT @@global.rpl_semi_sync_master_wait_no_slave AND @@global.rpl_semi_sync_master_timeout > 1000000",
    "DiscoverByShowSlaveHosts": true,
    "EnableSyslog": false,
    "ExpiryHostnameResolvesMinutes": 60,
    "FailureDetectionPeriodBlockMinutes": 60,
    "GraphiteAddr": "",
    "GraphiteConvertHostnameDotsToUnderscores": true,
    "GraphitePath": "",
    "HostnameResolveMethod": "none",
    "HTTPAuthPassword": "",
    "HTTPAuthUser": "",
    "InstanceBulkOperationsWaitTimeoutSeconds": 10,
    "InstancePollSeconds": 12,
    "ListenAddress": ":3000",
    "MasterFailoverLostInstancesDowntimeMinutes": 0,
    "MySQLConnectTimeoutSeconds": 1,
    "MySQLHostnameResolveMethod": "none",
    "MySQLTopologyCredentialsConfigFile": "",
    "MySQLTopologyMaxPoolConnections": 3,
    "MySQLTopologyPassword": "orc_client_user_password",
    "MySQLTopologyReadTimeoutSeconds": 3,
    "MySQLTopologySSLCAFile": "",
    "MySQLTopologySSLCertFile": "",
    "MySQLTopologySSLPrivateKeyFile": "",
    "MySQLTopologySSLSkipVerify": true,
    "MySQLTopologyUseMutualTLS": false,
    "MySQLTopologyUser": "orc_client_user",
    "OnFailureDetectionProcesses": [
        "echo 'Detected {failureType} on {failureCluster}. Affected replicas: {countSlaves}' >> /tmp/recovery.log"
    ],
    "OSCIgnoreHostnameFilters": [
    ],
    "PhysicalEnvironmentPattern": "[.]([^.]+[.][^.]+)[.]mydomain[.]com",
    "PostFailoverProcesses": [
        "echo '(for all types) Recovered from {failureType} on {failureCluster}. Failed: {failedHost}:{failedPort}; Successor: {successorHost}:{successorPort}' >> /tmp/recovery.log"
    ],
    "PostIntermediateMasterFailoverProcesses": [
        "echo 'Recovered from {failureType} on {failureCluster}. Failed: {failedHost}:{failedPort}; Successor: {successorHost}:{successorPort}' >> /tmp/recovery.log"
    ],
    "PostMasterFailoverProcesses": [
        "echo 'Recovered from {failureType} on {failureCluster}. Failed: {failedHost}:{failedPort}; Promoted: {successorHost}:{successorPort}' >> /tmp/recovery.log",
        "vtctlclient -server vtctld.{{ $namespace }}:15999 TabletExternallyReparented {successorAlias}"
    ],
    "PostponeSlaveRecoveryOnLagMinutes": 0,
    "PostUnsuccessfulFailoverProcesses": [
    ],
    "PowerAuthUsers": [
        "*"
    ],
    "PreFailoverProcesses": [
        "echo 'Will recover from {failureType} on {failureCluster}' >> /tmp/recovery.log"
    ],
    "ProblemIgnoreHostnameFilters": [
    ],
    "PromotionIgnoreHostnameFilters": [
    ],
    "PseudoGTIDMonotonicHint": "asc:",
    "PseudoGTIDPattern": "drop view if exists .*?`_pseudo_gtid_hint__",
    "RaftAdvertise": "POD_NAME.{{ $namespace }}",
    "RaftBind": "POD_NAME",
    "RaftDataDir": "/var/lib/orchestrator",
    "RaftEnabled": true,
    "RaftNodes": [
    {{ range $i := until (int $orc.replicas) }}
        "orchestrator-{{ $i }}.{{ $namespace }}"{{ if lt $i (sub (int64 $orc.replicas) 1) }},{{ end }}
    {{ end }}
    ],
    "ReadLongRunningQueries": false,
    "ReadOnly": false,
    "ReasonableMaintenanceReplicationLagSeconds": 20,
    "ReasonableReplicationLagSeconds": 10,
    "RecoverIntermediateMasterClusterFilters": [
        "_intermediate_master_pattern_"
    ],
    "RecoverMasterClusterFilters": [
        ".*"
    ],
    "RecoveryIgnoreHostnameFilters": [
    ],
    "RecoveryPeriodBlockMinutes": 1,
    "RecoveryPeriodBlockSeconds": 60,
    "ReduceReplicationAnalysisCount": true,
    "RejectHostnameResolvePattern": "",
    "RemoveTextFromHostnameDisplay": ".mydomain.com:3306",
    "ReplicationLagQuery": "select round(absolute_lag) from meta.heartbeat_view",
    "SeedAcceptableBytesDiff": 8192,
    "ServeAgentsHttp": false,
    "SkipBinlogEventsContaining": [
    ],
    "SkipBinlogServerUnresolveCheck": true,
    "SkipMaxScaleCheck": true,
    "SkipOrchestratorDatabaseUpdate": false,
    "SlaveLagQuery": "",
    "SlaveStartPostWaitMilliseconds": 1000,
    "SnapshotTopologiesIntervalHours": 0,
    "SQLite3DataFile": ":memory:",
    "SSLCAFile": "",
    "SSLCertFile": "",
    "SSLPrivateKeyFile": "",
    "SSLSkipVerify": false,
    "SSLValidOUs": [
    ],
    "StaleSeedFailMinutes": 60,
    "StatusEndpoint": "/api/status",
    "StatusOUVerify": false,
    "UnseenAgentForgetHours": 6,
    "UnseenInstanceForgetHours": 240,
    "UseMutualTLS": false,
    "UseSSL": false,
    "VerifyReplicationFilters": false
    }
{{ end }}