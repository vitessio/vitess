package topoproto

import (
	"bytes"
	"sort"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ShardReferenceArray is used for sorting ShardReference arrays
type ShardReferenceArray []*pb.ShardReference

// Len implements sort.Interface
func (sra ShardReferenceArray) Len() int { return len(sra) }

// Len implements sort.Interface
func (sra ShardReferenceArray) Less(i, j int) bool {
	return bytes.Compare(sra[i].KeyRange.Start, sra[j].KeyRange.Start) < 0
}

// Len implements sort.Interface
func (sra ShardReferenceArray) Swap(i, j int) {
	sra[i], sra[j] = sra[j], sra[i]
}

// Sort will sort the list according to KeyRange.Start
func (sra ShardReferenceArray) Sort() { sort.Sort(sra) }

// SrvKeyspaceGetPartition returns a Partition for the given tablet type,
// or nil if it's not there.
func SrvKeyspaceGetPartition(sk *pb.SrvKeyspace, tabletType pb.TabletType) *pb.SrvKeyspace_KeyspacePartition {
	for _, p := range sk.Partitions {
		if p.ServedType == tabletType {
			return p
		}
	}
	return nil
}
