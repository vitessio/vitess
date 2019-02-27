package pools

import (
	"context"
	"testing"
	"time"
)

func s(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func VerySlowFactory() (Resource, error) {
	s(10)
	return &VerySlowResource{}, nil
}

// VerySlowResource takes time to create and close.
type VerySlowResource struct{}

func (r *VerySlowResource) Close() {
	s(1)
}

func benchmarkGetPut(b *testing.B, impl Impl, f CreateFactory, workers int, cap int) {
	b.Helper()
	p := New(impl, f, cap, cap, time.Millisecond*10, 0)

	done := make(chan bool)
	for j := 0; j < workers; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				s(1)

				r, err := p.Get(context.Background())
				if err != nil {
					b.Error(err)
				}

				s(1)

				p.Put(r)
			}
			done <- true
		}()
	}

	for j := 0; j < workers; j++ {
		<-done
	}

	p.Close()
}

