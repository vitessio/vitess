/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtadmin2

import (
	"net/http"
	"strconv"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func (s *Server) gates(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetGates(r.Context(), &vtadminpb.GetGatesRequest{
		ClusterIds: queryValues(r, "cluster_id"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "VTGates", err)
		return
	}

	s.render(w, r, http.StatusOK, "gates.html", PageData{
		Title:  "VTGates",
		Active: "gates",
		Data:   resp.GetGates(),
	})
}

func (s *Server) vtctlds(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetVtctlds(r.Context(), &vtadminpb.GetVtctldsRequest{
		ClusterIds: queryValues(r, "cluster_id"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Vtctlds", err)
		return
	}

	s.render(w, r, http.StatusOK, "vtctlds.html", PageData{
		Title:  "Vtctlds",
		Active: "vtctlds",
		Data:   resp.GetVtctlds(),
	})
}

func (s *Server) cells(w http.ResponseWriter, r *http.Request) {
	namesOnly, err := parseQueryBool(r, "names_only", false)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Cells", err)
		return
	}

	resp, err := s.api.GetCellInfos(r.Context(), &vtadminpb.GetCellInfosRequest{
		ClusterIds: queryValues(r, "cluster_id"),
		Cells:      queryValues(r, "cell"),
		NamesOnly:  namesOnly,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Cells", err)
		return
	}

	s.render(w, r, http.StatusOK, "cells.html", PageData{
		Title:  "Cells",
		Active: "cells",
		Data:   resp.GetCellInfos(),
	})
}

func (s *Server) cellsAliases(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetCellsAliases(r.Context(), &vtadminpb.GetCellsAliasesRequest{
		ClusterIds: queryValues(r, "cluster_id"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Cell Aliases", err)
		return
	}

	s.render(w, r, http.StatusOK, "cells_aliases.html", PageData{
		Title:  "Cell Aliases",
		Active: "cells_aliases",
		Data:   resp.GetAliases(),
	})
}

func (s *Server) backups(w http.ResponseWriter, r *http.Request) {
	limit, err := parseQueryUint32(r, "limit", 0)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Backups", err)
		return
	}
	detailed, err := parseQueryBool(r, "detailed", true)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Backups", err)
		return
	}
	detailedLimit, err := parseQueryUint32(r, "detailed_limit", 3)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Backups", err)
		return
	}

	resp, err := s.api.GetBackups(r.Context(), &vtadminpb.GetBackupsRequest{
		ClusterIds:     queryValues(r, "cluster_id"),
		Keyspaces:      queryValues(r, "keyspace"),
		KeyspaceShards: queryValues(r, "keyspace_shard"),
		RequestOptions: &vtctldatapb.GetBackupsRequest{
			Limit:         limit,
			Detailed:      detailed,
			DetailedLimit: detailedLimit,
		},
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Backups", err)
		return
	}

	s.render(w, r, http.StatusOK, "backups.html", PageData{
		Title:  "Backups",
		Active: "backups",
		Data:   resp.GetBackups(),
	})
}

func parseQueryBool(r *http.Request, name string, defaultVal bool) (bool, error) {
	param := queryValue(r, name)
	if param == "" {
		return defaultVal, nil
	}
	val, err := strconv.ParseBool(param)
	if err != nil {
		return defaultVal, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse query parameter %s (= %s) into bool value", name, param)
	}
	return val, nil
}

func parseQueryUint32(r *http.Request, name string, defaultVal uint32) (uint32, error) {
	param := queryValue(r, name)
	if param == "" {
		return defaultVal, nil
	}
	val, err := strconv.ParseUint(param, 10, 32)
	if err != nil {
		return defaultVal, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse query parameter %s (= %s) into uint32 value", name, param)
	}
	return uint32(val), nil
}
