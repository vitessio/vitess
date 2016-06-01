package etcdtopo

import (
	"encoding/json"
	"fmt"

	"golang.org/x/net/context"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	"github.com/youtube/vitess/go/vt/topo"
)

/*
This file contains the vschema management code for etcdtopo.Server
*/

// SaveVSchema saves the JSON vschema into the topo.
func (s *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	data, err := json.MarshalIndent(vschema, "", "  ")
	if err != nil {
		return err
	}
	_, err = s.getGlobal().Set(vschemaFilePath(keyspace), string(data), 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetVSchema fetches the vschema from the topo.
func (s *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	resp, err := s.getGlobal().Get(vschemaFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return nil, topo.ErrNoNode
		}
		return nil, err
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}
	var vs vschemapb.Keyspace
	if err := json.Unmarshal([]byte(resp.Node.Value), &vs); err != nil {
		return nil, fmt.Errorf("bad vschema data (%v): %q", err, resp.Node.Value)
	}
	return &vs, nil
}
