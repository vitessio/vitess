package etcdtopo

import (
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	// vindexes needs to be imported so that they register
	// themselves against vtgate/planbuilder. This will allow
	// us to sanity check the schema being uploaded.
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

/*
This file contains the vschema management code for etcdtopo.Server
*/

// SaveVSchema saves the JSON vschema into the topo.
func (s *Server) SaveVSchema(vschema string) error {
	_, err := planbuilder.NewSchema([]byte(vschema))
	if err != nil {
		return err
	}

	_, err = s.getGlobal().Set(vschemaPath, vschema, 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetVSchema fetches the JSON vschema from the topo.
func (s *Server) GetVSchema() (string, error) {
	resp, err := s.getGlobal().Get(vschemaPath, false /* sort */, false /* recursive */)
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
