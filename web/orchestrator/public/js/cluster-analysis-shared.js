var interestingAnalysis = {
	"DeadPrimary" : true,
	"DeadPrimaryAndReplicas" : true,
	"DeadPrimaryAndSomeReplicas" : true,
	"DeadPrimaryWithoutReplicas" : true,
	"UnreachablePrimaryWithLaggingReplicas": true,
	"UnreachablePrimary" : true,
	"LockedSemiSyncPrimary" : true,
	"AllPrimaryReplicasNotReplicating" : true,
	"AllPrimaryReplicasNotReplicatingOrDead" : true,
	"DeadCoPrimary" : true,
	"DeadCoPrimaryAndSomeReplicas" : true,
	"DeadIntermediatePrimary" : true,
	"DeadIntermediatePrimaryWithSingleReplicaFailingToConnect" : true,
	"DeadIntermediatePrimaryWithSingleReplica" : true,
	"DeadIntermediatePrimaryAndSomeReplicas" : true,
	"DeadIntermediatePrimaryAndReplicas" : true,
	"AllIntermediatePrimaryReplicasFailingToConnectOrDead" : true,
	"AllIntermediatePrimaryReplicasNotReplicating" : true,
	"UnreachableIntermediatePrimaryWithLaggingReplicas": true,
	"UnreachableIntermediatePrimary" : true,
	"BinlogServerFailingToConnectToPrimary" : true,
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
