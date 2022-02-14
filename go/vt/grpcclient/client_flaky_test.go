package grpcclient

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

func TestDialErrors(t *testing.T) {
	addresses := []string{
		"badhost",
		"badhost:123456",
		"[::]:12346",
	}
	wantErr := "Unavailable"
	for _, address := range addresses {
		gconn, err := Dial(address, FailFast(true), grpc.WithInsecure())
		if err != nil {
			t.Fatal(err)
		}
		vtg := vtgateservicepb.NewVitessClient(gconn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err = vtg.Execute(ctx, &vtgatepb.ExecuteRequest{})
		cancel()
		gconn.Close()
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("Dial(%s, FailFast=true): %v, must contain %s", address, err, wantErr)
		}
	}

	wantErr = "DeadlineExceeded"
	for _, address := range addresses {
		gconn, err := Dial(address, FailFast(false), grpc.WithInsecure())
		if err != nil {
			t.Fatal(err)
		}
		vtg := vtgateservicepb.NewVitessClient(gconn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err = vtg.Execute(ctx, &vtgatepb.ExecuteRequest{})
		cancel()
		gconn.Close()
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("Dial(%s, FailFast=false): %v, must contain %s", address, err, wantErr)
		}
	}
}
