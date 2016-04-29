// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

//
// This file contains utility functions for clone workers.
//

// Does a topo lookup for a single shard, and returns:
//	1. Slice of all tablet aliases for the shard.
//	2. Map of tablet alias : tablet record for all tablets.
func resolveReloadTabletsForShard(ctx context.Context, keyspace, shard string, wr *wrangler.Wrangler) (reloadAliases []*topodatapb.TabletAlias, reloadTablets map[topodatapb.TabletAlias]*topo.TabletInfo, err error) {
	// Keep a long timeout, because we really don't want the copying to succeed, and then the worker to fail at the end.
	shortCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	reloadAliases, err = wr.TopoServer().FindAllTabletAliasesInShard(shortCtx, keyspace, shard)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot find all reload target tablets in %v/%v: %v", keyspace, shard, err)
	}
	wr.Logger().Infof("Found %v reload target aliases in shard %v/%v", len(reloadAliases), keyspace, shard)

	shortCtx, cancel = context.WithTimeout(ctx, 5*time.Minute)
	reloadTablets, err = wr.TopoServer().GetTabletMap(shortCtx, reloadAliases)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read all reload target tablets in %v/%v: %v",
			keyspace, shard, err)
	}
	return reloadAliases, reloadTablets, nil
}

var errExtract = regexp.MustCompile(`\(errno (\d+)\)`)

// executeFetchWithRetries will attempt to run ExecuteFetch for a single command, with a reasonably small timeout.
// If will keep retrying the ExecuteFetch (for a finite but longer duration) if it fails due to a timeout or a
// retriable application error.
//
// executeFetchWithRetries will always get the current MASTER tablet from the
// healthcheck instance. If no MASTER is available, it will keep retrying.
func executeFetchWithRetries(ctx context.Context, wr *wrangler.Wrangler, healthCheck discovery.HealthCheck, keyspace, shard, command string) error {
	retryDuration := 2 * time.Hour
	// We should keep retrying up until the retryCtx runs out.
	retryCtx, retryCancel := context.WithTimeout(ctx, retryDuration)
	defer retryCancel()
	// Is this current attempt a retry of a previous attempt?
	isRetry := false
	for {
		var master *discovery.EndPointStats
		var err error

		// Get the current master from the HealthCheck.
		masters := discovery.GetCurrentMaster(
			healthCheck.GetEndPointStatsFromTarget(keyspace, shard, topodatapb.TabletType_MASTER))
		if len(masters) == 0 {
			wr.Logger().Warningf("ExecuteFetch failed for keyspace/shard %v/%v because no MASTER is available; will retry until there is MASTER again", keyspace, shard)
			statsRetryCount.Add(1)
			statsRetryCounters.Add(retryCategoryNoMasterAvailable, 1)
			goto retry
		}
		master = masters[0]

		// Run the command.
		{
			tryCtx, cancel := context.WithTimeout(retryCtx, 2*time.Minute)
			_, err = wr.TabletManagerClient().ExecuteFetchAsApp(tryCtx, endPointToTabletInfo(master), command, 0)
			cancel()

			if err == nil {
				// success!
				return nil
			}

			succeeded, finalErr := checkError(wr, err, isRetry, master, keyspace, shard)
			if succeeded {
				// We can ignore the error and don't have to retry.
				return nil
			}
			if finalErr != nil {
				// Non-retryable error.
				return finalErr
			}
		}

	retry:
		masterAlias := "no-master-was-available"
		if master != nil {
			masterAlias = topoproto.TabletAliasString(master.Alias())
		}
		tabletString := fmt.Sprintf("%v (%v/%v)", masterAlias, keyspace, shard)

		select {
		case <-retryCtx.Done():
			if retryCtx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("failed to connect to destination tablet %v after retrying for %v", tabletString, retryDuration)
			}
			return fmt.Errorf("interrupted while trying to run %v on tablet %v", command, tabletString)
		case <-time.After(*executeFetchRetryTime):
			// Retry 30s after the failure using the current master seen by the HealthCheck.
		}
		isRetry = true
	}
}

// checkError returns true if the error can be ignored and the command
// succeeded, false if the error is retryable and a non-nil error if the
// command must not be retried.
func checkError(wr *wrangler.Wrangler, err error, isRetry bool, master *discovery.EndPointStats, keyspace, shard string) (bool, error) {
	tabletString := fmt.Sprintf("%v (%v/%v)", topoproto.TabletAliasString(master.Alias()), keyspace, shard)
	// If the ExecuteFetch call failed because of an application error, we will try to figure out why.
	// We need to extract the MySQL error number, and will attempt to retry if we think the error is recoverable.
	match := errExtract.FindStringSubmatch(err.Error())
	var errNo string
	if len(match) == 2 {
		errNo = match[1]
	}
	switch {
	case wr.TabletManagerClient().IsTimeoutError(err):
		wr.Logger().Warningf("ExecuteFetch failed on %v; will retry because it was a timeout error: %v", tabletString, err)
		statsRetryCount.Add(1)
		statsRetryCounters.Add(retryCategoryTimeoutError, 1)
	case errNo == "1290":
		wr.Logger().Warningf("ExecuteFetch failed on %v; will reresolve and retry because it's due to a MySQL read-only error: %v", tabletString, err)
		statsRetryCount.Add(1)
		statsRetryCounters.Add(retryCategoryReadOnly, 1)
	case errNo == "2002" || errNo == "2006":
		wr.Logger().Warningf("ExecuteFetch failed on %v; will reresolve and retry because it's due to a MySQL connection error: %v", tabletString, err)
		statsRetryCount.Add(1)
		statsRetryCounters.Add(retryCategoryConnectionError, 1)
	case errNo == "1062":
		if !isRetry {
			return false, fmt.Errorf("ExecuteFetch failed on %v on the first attempt; not retrying as this is not a recoverable error: %v", tabletString, err)
		}
		wr.Logger().Infof("ExecuteFetch failed on %v with a duplicate entry error; marking this as a success, because of the likelihood that this query has already succeeded before being retried: %v", tabletString, err)
		return true, nil
	default:
		// Unknown error.
		return false, err
	}
	return false, nil
}

// fillStringTemplate returns the string template filled
func fillStringTemplate(tmpl string, vars interface{}) (string, error) {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}

// runSQLCommands will send the sql commands to the remote tablet.
func runSQLCommands(ctx context.Context, wr *wrangler.Wrangler, healthCheck discovery.HealthCheck, keyspace, shard, dbName string, commands []string) error {
	for _, command := range commands {
		command, err := fillStringTemplate(command, map[string]string{"DatabaseName": dbName})
		if err != nil {
			return fmt.Errorf("fillStringTemplate failed: %v", err)
		}

		if err := executeFetchWithRetries(ctx, wr, healthCheck, keyspace, shard, command); err != nil {
			return err
		}
	}

	return nil
}

// FindChunks returns an array of chunks to use for splitting up a table
// into multiple data chunks. It only works for tables with a primary key
// (and the primary key first column is an integer type).
// The array will always look like:
// "", "value1", "value2", ""
// A non-split tablet will just return:
// "", ""
func FindChunks(ctx context.Context, wr *wrangler.Wrangler, ti *topo.TabletInfo, td *tabletmanagerdatapb.TableDefinition, minTableSizeForSplit uint64, sourceReaderCount int) ([]string, error) {
	result := []string{"", ""}

	// eliminate a few cases we don't split tables for
	if len(td.PrimaryKeyColumns) == 0 {
		// no primary key, what can we do?
		return result, nil
	}
	if td.DataLength < minTableSizeForSplit {
		// table is too small to split up
		return result, nil
	}

	// get the min and max of the leading column of the primary key
	query := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v.%v", td.PrimaryKeyColumns[0], td.PrimaryKeyColumns[0], ti.DbName(), td.Name)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	qr, err := wr.TabletManagerClient().ExecuteFetchAsApp(shortCtx, ti, query, 1)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("ExecuteFetchAsApp: %v", err)
	}
	if len(qr.Rows) != 1 {
		wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot get min and max", td.Name)
		return result, nil
	}

	// FIXME(alainjobart) this code is a bit clunky. I'd like to
	// convert the first row into an array of Values, and then
	// see which type they are and go from there. Can only happen after
	// Value has a full type.
	l0 := qr.Rows[0].Lengths[0]
	l1 := qr.Rows[0].Lengths[1]
	if l0 < 0 || l1 < 0 {
		wr.Logger().Infof("Not splitting table %v into multiple chunks, min or max is NULL: %v", td.Name, qr.Rows[0])
		return result, nil
	}
	minValue := qr.Rows[0].Values[:l0]
	maxValue := qr.Rows[0].Values[l0 : l0+l1]
	switch {
	case sqltypes.IsSigned(qr.Fields[0].Type):
		min, err := strconv.ParseInt(string(minValue), 10, 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, string(minValue), err)
			return result, nil
		}
		max, err := strconv.ParseInt(string(maxValue), 10, 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, string(maxValue), err)
			return result, nil
		}
		interval := (max - min) / int64(sourceReaderCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", td.Name, max, min)
			return result, nil
		}

		result = make([]string, sourceReaderCount+1)
		result[0] = ""
		result[sourceReaderCount] = ""
		for i := int64(1); i < int64(sourceReaderCount); i++ {
			result[i] = fmt.Sprintf("%v", min+interval*i)
		}
		return result, nil

	case sqltypes.IsUnsigned(qr.Fields[0].Type):
		min, err := strconv.ParseUint(string(minValue), 10, 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, string(minValue), err)
			return result, nil
		}
		max, err := strconv.ParseUint(string(maxValue), 10, 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, string(maxValue), err)
			return result, nil
		}
		interval := (max - min) / uint64(sourceReaderCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", td.Name, max, min)
			return result, nil
		}

		result = make([]string, sourceReaderCount+1)
		result[0] = ""
		result[sourceReaderCount] = ""
		for i := uint64(1); i < uint64(sourceReaderCount); i++ {
			result[i] = fmt.Sprintf("%v", min+interval*i)
		}
		return result, nil

	case sqltypes.IsFloat(qr.Fields[0].Type):
		min, err := strconv.ParseFloat(string(minValue), 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, string(minValue), err)
			return result, nil
		}
		max, err := strconv.ParseFloat(string(maxValue), 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, string(maxValue), err)
			return result, nil
		}
		interval := (max - min) / float64(sourceReaderCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", td.Name, max, min)
			return result, nil
		}

		result = make([]string, sourceReaderCount+1)
		result[0] = ""
		result[sourceReaderCount] = ""
		for i := 1; i < sourceReaderCount; i++ {
			result[i] = fmt.Sprintf("%v", min+interval*float64(i))
		}
		return result, nil
	}

	wr.Logger().Infof("Not splitting table %v into multiple chunks, primary key not numeric", td.Name)
	return result, nil
}

// buildSQLFromChunks returns the SQL command to run to insert the data
// using the chunks definitions into the provided table.
func buildSQLFromChunks(wr *wrangler.Wrangler, td *tabletmanagerdatapb.TableDefinition, chunks []string, chunkIndex int, source string) string {
	selectSQL := "SELECT " + strings.Join(td.Columns, ", ") + " FROM " + td.Name
	if chunks[chunkIndex] != "" || chunks[chunkIndex+1] != "" {
		wr.Logger().Infof("Starting to stream all data from tablet %v table %v between '%v' and '%v'", source, td.Name, chunks[chunkIndex], chunks[chunkIndex+1])
		clauses := make([]string, 0, 2)
		if chunks[chunkIndex] != "" {
			clauses = append(clauses, td.PrimaryKeyColumns[0]+">="+chunks[chunkIndex])
		}
		if chunks[chunkIndex+1] != "" {
			clauses = append(clauses, td.PrimaryKeyColumns[0]+"<"+chunks[chunkIndex+1])
		}
		selectSQL += " WHERE " + strings.Join(clauses, " AND ")
	} else {
		wr.Logger().Infof("Starting to stream all data from tablet %v table %v", source, td.Name)
	}
	if len(td.PrimaryKeyColumns) > 0 {
		selectSQL += " ORDER BY " + strings.Join(td.PrimaryKeyColumns, ", ")
	}
	return selectSQL
}

// makeValueString returns a string that contains all the passed-in rows
// as an insert SQL command's parameters.
func makeValueString(fields []*querypb.Field, rows [][]sqltypes.Value) string {
	buf := bytes.Buffer{}
	for i, row := range rows {
		if i > 0 {
			buf.Write([]byte(",("))
		} else {
			buf.WriteByte('(')
		}
		for j, value := range row {
			if j > 0 {
				buf.WriteByte(',')
			}
			value.EncodeSQL(&buf)
		}
		buf.WriteByte(')')
	}
	return buf.String()
}

// executeFetchLoop loops over the provided insertChannel
// and sends the commands to the provided tablet.
func executeFetchLoop(ctx context.Context, wr *wrangler.Wrangler, healthCheck discovery.HealthCheck, keyspace, shard, dbName string, insertChannel chan string) error {
	for {
		select {
		case cmd, ok := <-insertChannel:
			if !ok {
				// no more to read, we're done
				return nil
			}
			cmd = "INSERT INTO `" + dbName + "`." + cmd
			if err := executeFetchWithRetries(ctx, wr, healthCheck, keyspace, shard, cmd); err != nil {
				return fmt.Errorf("ExecuteFetch failed: %v", err)
			}
		case <-ctx.Done():
			// Doesn't really matter if this select gets starved, because the other case
			// will also return an error due to executeFetch's context being closed. This case
			// does prevent us from blocking indefinitely on insertChannel when the worker is canceled.
			return nil
		}
	}
}

// endPointToTabletInfo converts an EndPointStats object from the discovery
// package into a TabletInfo object. The latter one is required by several
// TabletManagerClient API calls.
// Note that this is a best-effort conversion and won't result into the same
// result as a call to topo.GetTablet().
// Note: We assume that "eps" is immutable and we can reference its data.
func endPointToTabletInfo(eps *discovery.EndPointStats) *topo.TabletInfo {
	return topo.NewTabletInfo(&topodatapb.Tablet{
		Alias:     eps.Alias(),
		Hostname:  eps.EndPoint.Host,
		PortMap:   eps.EndPoint.PortMap,
		HealthMap: eps.EndPoint.HealthMap,
		Keyspace:  eps.Target.Keyspace,
		Shard:     eps.Target.Shard,
		Type:      eps.Target.TabletType,
	}, -1 /* version */)
}
