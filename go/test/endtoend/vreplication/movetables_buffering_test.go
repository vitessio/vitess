package vreplication

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/wrangler"
)

func TestMoveTablesBuffering(t *testing.T) {
	defaultRdonly = 1
	vc = setupMinimalCluster(t)
	defer vtgateConn.Close()
	defer vc.TearDown(t)

	currentWorkflowType = wrangler.MoveTablesWorkflow
	setupMinimalCustomerKeyspace(t)
	tables := "loadtest"
	err := tstWorkflowExec(t, defaultCellName, workflowName, sourceKs, targetKs,
		tables, workflowActionCreate, "", "", "")
	require.NoError(t, err)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	loadCtx, cancelLoad := context.WithCancel(context.Background())
	go func() {
		startLoad(t, loadCtx)
	}()
	time.Sleep(2 * time.Second) // wait for enough records to be inserted by startLoad

	catchup(t, targetTab1, workflowName, "MoveTables")
	catchup(t, targetTab2, workflowName, "MoveTables")
	vdiff1(t, ksWorkflow, "")
	waitForLowLag(t, "customer", workflowName)
	tstWorkflowSwitchReads(t, "", "")
	tstWorkflowSwitchWrites(t)
	log.Infof("SwitchWrites done")
	stopLoad(t, cancelLoad)

	log.Infof("TestMoveTablesBuffering: done")
	log.Flush()
}

func stopLoad(t *testing.T, cancel context.CancelFunc) {
	time.Sleep(11 * time.Second) // wait for buffering to stop and additional records to be inserted by startLoad after traffic is switched
	log.Infof("Canceling load")
	cancel()
	time.Sleep(2 * time.Second) // wait for cancel to take effect
	log.Flush()

}
func startLoad(t *testing.T, ctx context.Context) {
	var id int64
	log.Infof("startLoad: starting")
	queryTemplate := "insert into loadtest(id, name) values (%d, 'name-%d')"
	var totalQueries, successfulQueries int64
	var deniedErrors, ambiguousErrors, reshardedErrors, tableNotFoundErrors, otherErrors int64
	defer func() {

		log.Infof("startLoad: totalQueries: %d, successfulQueries: %d, deniedErrors: %d, ambiguousErrors: %d, reshardedErrors: %d, tableNotFoundErrors: %d, otherErrors: %d",
			totalQueries, successfulQueries, deniedErrors, ambiguousErrors, reshardedErrors, tableNotFoundErrors, otherErrors)
	}()
	logOnce := true
	for {
		select {
		case <-ctx.Done():
			log.Infof("startLoad: context cancelled")
			log.Infof("startLoad: deniedErrors: %d, ambiguousErrors: %d, reshardedErrors: %d, tableNotFoundErrors: %d, otherErrors: %d",
				deniedErrors, ambiguousErrors, reshardedErrors, tableNotFoundErrors, otherErrors)
			require.Less(t, deniedErrors, int64(1))
			require.Less(t, otherErrors, int64(1))
			require.Equal(t, totalQueries, successfulQueries)
			return
		default:
			go func() {
				conn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
				defer conn.Close()
				atomic.AddInt64(&id, 1)
				query := fmt.Sprintf(queryTemplate, id, id)
				_, err := conn.ExecuteFetch(query, 1, false)
				atomic.AddInt64(&totalQueries, 1)
				if err != nil {
					sqlErr := err.(*mysql.SQLError)
					if strings.Contains(strings.ToLower(err.Error()), "denied tables") {
						log.Infof("startLoad: denied tables error executing query: %d:%v", sqlErr.Number(), err)
						atomic.AddInt64(&deniedErrors, 1)
					} else if strings.Contains(strings.ToLower(err.Error()), "ambiguous") {
						// this can happen when a second keyspace is setup with the same tables, but there are no routing rules
						// set yet by MoveTables. So we ignore these errors.
						atomic.AddInt64(&ambiguousErrors, 1)
						log.Flush()
						//panic(err)
					} else if strings.Contains(strings.ToLower(err.Error()), "current keyspace is being resharded") {
						atomic.AddInt64(&reshardedErrors, 1)
					} else if strings.Contains(strings.ToLower(err.Error()), "not found") {
						atomic.AddInt64(&tableNotFoundErrors, 1)
					} else {
						if logOnce {
							log.Infof("startLoad: error executing query: %d:%v", sqlErr.Number(), err)
							logOnce = false
						}
						atomic.AddInt64(&otherErrors, 1)
					}
					//log.Infof("startLoad: totalQueries: %d, successfulQueries: %d, deniedErrors: %d, ambiguousErrors: %d, reshardedErrors: %d, tableNotFoundErrors: %d, otherErrors: %d",
					//	totalQueries, successfulQueries, deniedErrors, ambiguousErrors, reshardedErrors, tableNotFoundErrors, otherErrors)
					//log.Flush()
					//panic(err)
					time.Sleep(2 * time.Millisecond)
				} else {
					atomic.AddInt64(&successfulQueries, 1)
				}
			}()
			time.Sleep(2 * time.Millisecond)
		}
	}
}
