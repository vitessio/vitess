package txlimiter

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/callerid"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func resetVariables() {
	rejections.Reset()
	rejectionsDryRun.Reset()
}

func createCtx(username, principal, component, subcomponent string) context.Context {
	im := callerid.NewImmediateCallerID(username)
	ef := callerid.NewEffectiveCallerID(principal, component, subcomponent)
	return callerid.NewContext(context.Background(), ef, im)
}

func extractCaller(ctx context.Context) (*querypb.VTGateCallerID, *vtrpcpb.CallerID) {
	return callerid.ImmediateCallerIDFromContext(ctx), callerid.EffectiveCallerIDFromContext(ctx)
}

func TestTxLimiter_DisabledAllowsAll(t *testing.T) {
	limiter := New(10, 0.1, false, false, false, false, false, false)
	ctx := createCtx("", "", "", "")
	for i := 0; i < 5; i++ {
		if got, want := limiter.Get(ctx), true; got != want {
			t.Errorf("Transaction number %d, Get(): got %v, want %v", i, got, want)
		}
	}

}

func TestTxLimiter_LimitsOnlyOffendingUser(t *testing.T) {
	resetVariables()

	// This should allow 3 slots to all users
	newlimiter := New(10, 0.3, true, false, true, false, false, false)
	limiter, ok := newlimiter.(*Impl)
	if !ok {
		t.Fatalf("New returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	}
	ctx1 := createCtx("user1", "", "", "")
	ctx2 := createCtx("user2", "", "", "")

	// user1 uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(ctx1), true; got != want {
			t.Errorf("Transaction number %d, Get(ctx1): got %v, want %v", i, got, want)
		}
	}

	// user1 not allowed to use 4th slot, which increases counter
	if got, want := limiter.Get(ctx1), false; got != want {
		t.Errorf("Get(ctx1) after using up all allowed attempts: got %v, want %v", got, want)
	}

	key1 := limiter.extractKeyFromContext(ctx1)
	if got, want := rejections.Counts()[key1], int64(1); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key1, got, want)
	}

	// user2 uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(ctx2), true; got != want {
			t.Errorf("Transaction number %d, Get(ctx2): got %v, want %v", i, got, want)
		}
	}

	// user2 not allowed to use 4th slot, which increases counter
	if got, want := limiter.Get(ctx2), false; got != want {
		t.Errorf("Get(ctx2) after using up all allowed attempts: got %v, want %v", got, want)
	}
	key2 := limiter.extractKeyFromContext(ctx2)
	if got, want := rejections.Counts()[key2], int64(1); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key2, got, want)
	}

	// user1 releases a slot, which allows to get another
	limiter.Release(extractCaller(ctx1))
	if got, want := limiter.Get(ctx1), true; got != want {
		t.Errorf("Get(ctx1) after releasing: got %v, want %v", got, want)
	}

	// Rejection coutner for user 1 should still be 1.
	if got, want := rejections.Counts()[key1], int64(1); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key1, got, want)
	}
}

func TestTxLimiterDryRun(t *testing.T) {
	resetVariables()

	// This should allow 3 slots to all users
	newlimiter := New(10, 0.3, true, true, true, false, false, false)
	limiter, ok := newlimiter.(*Impl)
	if !ok {
		t.Fatalf("New returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	}
	ctx := createCtx("user", "", "", "")
	key := limiter.extractKeyFromContext(ctx)

	// uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(ctx), true; got != want {
			t.Errorf("Transaction number %d, Get(ctx): got %v, want %v", i, got, want)
		}
	}

	// allowed to use 4th slot, but dry run rejection counter increased
	if got, want := limiter.Get(ctx), true; got != want {
		t.Errorf("Get(ctx) after using up all allowed attempts: got %v, want %v", got, want)
	}

	if got, want := rejections.Counts()[key], int64(0); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key, got, want)
	}
	if got, want := rejectionsDryRun.Counts()[key], int64(1); got != want {
		t.Errorf("RejectionsDryRun count for %s: got %d, want %d", key, got, want)
	}
}
