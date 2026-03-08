package sandboxconn

import (
	"context"
	"errors"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestExecuteHonorsCanceledContextDuringExecDelay(t *testing.T) {
	sbc := NewSandboxConn(&topodatapb.Tablet{Type: topodatapb.TabletType_PRIMARY})
	sbc.ExecDelayResponse = time.Second
	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := sbc.Execute(ctx, nil, target, "select 1", nil, 0, 0, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want %v", err, context.Canceled)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Execute waited %v after context cancellation", elapsed)
	}
}

func TestStreamExecuteHonorsCanceledContextDuringExecDelay(t *testing.T) {
	sbc := NewSandboxConn(&topodatapb.Tablet{Type: topodatapb.TabletType_PRIMARY})
	sbc.ExecDelayResponse = time.Second
	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	callbackCalled := false
	start := time.Now()
	err := sbc.StreamExecute(ctx, nil, target, "select 1", nil, 0, 0, nil, func(*sqltypes.Result) error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StreamExecute error = %v, want %v", err, context.Canceled)
	}
	if callbackCalled {
		t.Fatal("StreamExecute callback was called after context cancellation")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("StreamExecute waited %v after context cancellation", elapsed)
	}

	sbc.ExecDelayResponse = 0
	err = sbc.StreamExecute(context.Background(), nil, target, "select 1", nil, 0, 0, nil, func(*sqltypes.Result) error {
		callbackCalled = true
		return nil
	})
	if err != nil {
		t.Fatalf("StreamExecute retry error = %v", err)
	}
	if !callbackCalled {
		t.Fatal("StreamExecute callback was not called on retry")
	}
}
