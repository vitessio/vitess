package topoproto

import pb "github.com/youtube/vitess/go/vt/proto/topodata"

func SrvKeyspaceGetPartition(sk *pb.SrvKeyspace, tabletType pb.TabletType) *pb.SrvKeyspace_KeyspacePartition {
	for _, p := range sk.Partitions {
		if p.ServedType == tabletType {
			return p
		}
	}
	return nil
}
