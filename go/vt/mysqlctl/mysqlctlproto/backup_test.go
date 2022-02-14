package mysqlctlproto

import (
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type backupHandle struct {
	backupstorage.BackupHandle
	name      string
	directory string
}

func (bh *backupHandle) Name() string      { return bh.name }
func (bh *backupHandle) Directory() string { return bh.directory }
func (bh *backupHandle) testname() string  { return path.Join(bh.directory, bh.name) }

func TestBackupHandleToProto(t *testing.T) {
	t.Parallel()

	now := time.Date(2021, time.June, 12, 15, 4, 5, 0, time.UTC)
	tests := []struct {
		bh   *backupHandle
		want *mysqlctlpb.BackupInfo
	}{
		{
			bh: &backupHandle{
				name:      "2021-06-12.150405.zone1-100",
				directory: "foo",
			},
			want: &mysqlctlpb.BackupInfo{
				Name:      "2021-06-12.150405.zone1-100",
				Directory: "foo",
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Time: protoutil.TimeToProto(now),
			},
		},
		{
			bh: &backupHandle{
				name:      "bar",
				directory: "foo",
			},
			want: &mysqlctlpb.BackupInfo{
				Name:      "bar",
				Directory: "foo",
			},
		},
		{
			bh: &backupHandle{
				name:      "invalid.time.zone1-100",
				directory: "foo",
			},
			want: &mysqlctlpb.BackupInfo{
				Name:      "invalid.time.zone1-100",
				Directory: "foo",
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Time: nil,
			},
		},
		{
			bh: &backupHandle{
				name:      "2021-06-12.150405.not_an_alias",
				directory: "foo",
			},
			want: &mysqlctlpb.BackupInfo{
				Name:        "2021-06-12.150405.not_an_alias",
				Directory:   "foo",
				TabletAlias: nil,
				Time:        protoutil.TimeToProto(now),
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.bh.testname(), func(t *testing.T) {
			t.Parallel()

			got := BackupHandleToProto(tt.bh)
			utils.MustMatch(t, tt.want, got)
		})
	}
}
