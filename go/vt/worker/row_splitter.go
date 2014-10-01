package worker

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

// RowSplitter is a helper function to split rows into multiple
// subsets targetted to different shards.
type RowSplitter struct {
	Type       key.KeyspaceIdType
	ValueIndex int
	KeyRanges  []key.KeyRange
}

// NewRowSplitter returns a new row splitter for the given shard distribution.
func NewRowSplitter(shardInfos []*topo.ShardInfo, typ key.KeyspaceIdType, valueIndex int) *RowSplitter {
	result := &RowSplitter{
		Type:       typ,
		ValueIndex: valueIndex,
		KeyRanges:  make([]key.KeyRange, len(shardInfos)),
	}
	for i, si := range shardInfos {
		result.KeyRanges[i] = si.KeyRange
	}
	return result
}

// Split will split the rows into subset for each distribution
func (rs *RowSplitter) Split(qr *mproto.QueryResult) ([][][]sqltypes.Value, error) {
	result := make([][][]sqltypes.Value, len(rs.KeyRanges))
	if rs.Type == key.KIT_UINT64 {
		for _, row := range qr.Rows {
			v := sqltypes.MakeNumeric(row[rs.ValueIndex].Raw())
			i, err := v.ParseUint64()
			if err != nil {
				return nil, fmt.Errorf("Non numerical value: %v", err)
			}
			k := key.Uint64Key(i).KeyspaceId()
			for i, kr := range rs.KeyRanges {
				if kr.Contains(k) {
					result[i] = append(result[i], row)
					break
				}
			}
		}
	} else {
		return nil, fmt.Errorf("NYI: string keys")
	}
	return result, nil
}
