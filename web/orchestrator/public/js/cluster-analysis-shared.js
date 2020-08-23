var interestingAnalysis = {
	"DeadMaster" : true,
	"DeadMasterAndReplicas" : true,
	"DeadMasterAndSomeReplicas" : true,
	"DeadMasterWithoutReplicas" : true,
	"UnreachableMasterWithLaggingReplicas": true,
	"UnreachableMaster" : true,
	"LockedSemiSyncMaster" : true,
	"AllMasterReplicasNotReplicating" : true,
	"AllMasterReplicasNotReplicatingOrDead" : true,
	"DeadCoMaster" : true,
	"DeadCoMasterAndSomeReplicas" : true,
	"DeadIntermediateMaster" : true,
	"DeadIntermediateMasterWithSingleReplicaFailingToConnect" : true,
	"DeadIntermediateMasterWithSingleReplica" : true,
	"DeadIntermediateMasterAndSomeReplicas" : true,
	"DeadIntermediateMasterAndReplicas" : true,
	"AllIntermediateMasterReplicasFailingToConnectOrDead" : true,
	"AllIntermediateMasterReplicasNotReplicating" : true,
	"UnreachableIntermediateMasterWithLaggingReplicas": true,
	"UnreachableIntermediateMaster" : true,
	"BinlogServerFailingToConnectToMaster" : true,
};

var errorMapping = {
	"in_maintenance": {
		"badge": "label-info",
		"description": "In maintenance"
	},
	"last_check_invalid": {
		"badge": "label-fatal",
		"description": "Last check invalid"
	},
	"not_recently_checked": {
		"badge": "label-stale",
		"description": "Not recently checked (stale)"
	},
	"not_replicating": {
		"badge": "label-danger",
		"description": "Not replicating"
	},
	"replication_lag": {
		"badge": "label-warning",
		"description": "Replication lag"
	},
	"errant_gtid": {
		"badge": "label-errant",
		"description": "Errant GTID"
	},
	"group_replication_member_not_online": {
		"badge": "label-danger",
		"description": "Replication group member is not ONLINE"
	}
};
