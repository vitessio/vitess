/*
Copyright 2019 The Vitess Authors.

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

package schema

import (
	"context"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
)

type HistorianInterface interface {
	RegisterVersionEvent(version *binlogdata.Version)
	GetTableForPos(tableName sqlparser.TableIdent, pos string) *Table
}

type HistoryEngine interface {
	Reload(ctx context.Context) error
	GetTable(ident sqlparser.TableIdent) *Table
}

type Historian struct {
	se HistoryEngine
}

//TODO first time called warm cache, drop cache on last
func NewSchemaHistorian(se HistoryEngine) *Historian {
	return &Historian{se: se}
}

//Placeholder TODO, currently returns Table not TableMetaData
func (h *Historian) GetTableForPos(tableName sqlparser.TableIdent, pos string) *Table {
	h.se.Reload(context.Background())
	_ = pos
	return h.se.GetTable(tableName)
}

func (h *Historian) RegisterVersionEvent(version *binlogdata.Version) {
}
