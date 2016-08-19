// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"regexp"
	"text/template"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

//
// This file contains utility functions for clone workers.
//

// Does a topo lookup for a single shard, and returns:
//	1. Slice of all tablet aliases for the shard.
//	2. Map of tablet alias : tablet record for all tablets.
func resolveRefreshTabletsForShard(ctx context.Context, keyspace, shard string, wr *wrangler.Wrangler) (refreshAliases []*topodatapb.TabletAlias, refreshTablets map[topodatapb.TabletAlias]*topo.TabletInfo, err error) {
	// Keep a long timeout, because we really don't want the copying to succeed, and then the worker to fail at the end.
	shortCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	refreshAliases, err = wr.TopoServer().FindAllTabletAliasesInShard(shortCtx, keyspace, shard)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot find all refresh target tablets in %v/%v: %v", keyspace, shard, err)
	}
	wr.Logger().Infof("Found %v refresh target aliases in shard %v/%v", len(refreshAliases), keyspace, shard)

	shortCtx, cancel = context.WithTimeout(ctx, 5*time.Minute)
	refreshTablets, err = wr.TopoServer().GetTabletMap(shortCtx, refreshAliases)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read all refresh target tablets in %v/%v: %v",
			keyspace, shard, err)
	}
	return refreshAliases, refreshTablets, nil
}

var errExtract = regexp.MustCompile(`\(errno (\d+)\)`)

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
func runSQLCommands(ctx context.Context, wr *wrangler.Wrangler, tsc *discovery.TabletStatsCache, keyspace, shard, dbName string, commands []string) error {
	for _, command := range commands {
		command, err := fillStringTemplate(command, map[string]string{"DatabaseName": dbName})
		if err != nil {
			return fmt.Errorf("fillStringTemplate failed: %v", err)
		}

		executor := newExecutor(wr, tsc, nil /* throttler */, keyspace, shard, 0 /* threadID */)
		if err := executor.fetchWithRetries(ctx, command); err != nil {
			return err
		}
	}

	return nil
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

// escape adds surrounding backticks (`) to an MySQL identifier.
// This is required for identifiers which are reserved words e.g. "CREATE".
func escape(identifier string) string {
	b := bytes.Buffer{}
	writeEscaped(&b, identifier)
	return b.String()
}

// escapeAll runs escape() for all entries in the slice.
func escapeAll(identifiers []string) []string {
	result := make([]string, len(identifiers))
	for i := range identifiers {
		result[i] = escape(identifiers[i])
	}
	return result
}

// writeEscaped escapes the SQL "identifier" before writing it to "b".
// See also escape().
func writeEscaped(b *bytes.Buffer, identifier string) {
	b.WriteByte('`')
	b.WriteString(identifier)
	b.WriteByte('`')
}
