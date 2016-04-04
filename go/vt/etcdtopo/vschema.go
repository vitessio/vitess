package etcdtopo

import (
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"golang.org/x/net/context"
)

/*
This file contains the vschema management code for etcdtopo.Server
*/

// SaveVSchema saves the JSON vschema into the topo.
func (s *Server) SaveVSchema(ctx context.Context, keyspace, vschema string) error {
	err := vindexes.ValidateVSchema([]byte(vschema))
	if err != nil {
		return err
	}

	_, err = s.getGlobal().Set(vschemaFilePath(keyspace), vschema, 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetVSchema fetches the JSON vschema from the topo.
func (s *Server) GetVSchema(ctx context.Context, keyspace string) (string, error) {
	resp, err := s.getGlobal().Get(vschemaFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return "{}", nil
		}
		return "", err
	}
	if resp.Node == nil {
		return "", ErrBadResponse
	}
	return resp.Node.Value, nil
}
