package vtadmin

import (
	"database/sql"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// ParseTablets converts a set of *sql.Rows into a slice of Tablets, for the
// given cluster.
func ParseTablets(rows *sql.Rows, c *cluster.Cluster) ([]*vtadminpb.Tablet, error) {
	var tablets []*vtadminpb.Tablet

	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}

		tablet, err := parseTablet(rows, c)
		if err != nil {
			return nil, err
		}

		tablets = append(tablets, tablet)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablets, nil
}

// Fields are:
// Cell | Keyspace | Shard | TabletType (string) | ServingState (string) | Alias | Hostname | MasterTermStartTime.
func parseTablet(rows *sql.Rows, c *cluster.Cluster) (*vtadminpb.Tablet, error) {
	var (
		cell            string
		tabletTypeStr   string
		servingStateStr string
		aliasStr        string
		mtstStr         string
		topotablet      topodatapb.Tablet

		err error
	)

	if err := rows.Scan(
		&cell,
		&topotablet.Keyspace,
		&topotablet.Shard,
		&tabletTypeStr,
		&servingStateStr,
		&aliasStr,
		&topotablet.Hostname,
		&mtstStr,
	); err != nil {
		return nil, err
	}

	tablet := &vtadminpb.Tablet{
		Cluster: &vtadminpb.Cluster{
			Id:   c.ID,
			Name: c.Name,
		},
		Tablet: &topotablet,
	}

	topotablet.Type, err = topoproto.ParseTabletType(tabletTypeStr)
	if err != nil {
		return nil, err
	}

	tablet.State = vtadminproto.ParseTabletServingState(servingStateStr)

	topotablet.Alias, err = topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return nil, err
	}

	if topotablet.Alias.Cell != cell {
		// (TODO:@amason) ???
		log.Warningf("tablet cell %s does not match alias %s. ignoring for now", cell, topoproto.TabletAliasString(topotablet.Alias))
	}

	if mtstStr != "" {
		timeTime, err := time.Parse(time.RFC3339, mtstStr)
		if err != nil {
			return nil, err
		}

		topotablet.MasterTermStartTime = logutil.TimeToProto(timeTime)
	}

	return tablet, nil
}
