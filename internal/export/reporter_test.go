package export

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

func TestMockReporterModes(t *testing.T) {
	ctx := context.Background()

	// OK by default.
	r0 := NewMockReporter()
	if res := r0.Report(ctx, nil); !res.OK {
		t.Fatal("default reporter must accept")
	}

	// Deterministic modulo failures: only every 3rd request fails.
	r := NewMockReporter()
	r.SetBehavior(Behavior{Mode: ModeFail, Failure: model.FailEndpointRejected, FailModulo: 3})
	results := make([]bool, 6)
	for i := range results {
		results[i] = r.Report(ctx, nil).OK
	}
	want := []bool{true, true, false, true, true, false}
	for i := range want {
		if results[i] != want[i] {
			t.Fatalf("attempt %d: ok=%v want %v", i+1, results[i], want[i])
		}
	}

	// Slow mode honors a short ctx and classifies it as a timeout.
	r.SetBehavior(Behavior{Mode: ModeSlow, Delay: 100 * time.Millisecond})
	cctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	res := r.Report(cctx, nil)
	if res.OK || res.Failure != model.FailEndpointTimeout {
		t.Fatalf("slow endpoint result=%+v", res)
	}

	// In-flight cap saturates deterministically into rejections.
	r.SetBehavior(Behavior{Mode: ModeSlow, Delay: 200 * time.Millisecond, InFlight: 1})
	errs := make(chan ReportResult, 4)
	blocker := make(chan struct{})
	go func() {
		c, c2 := context.WithCancel(context.Background())
		defer c2()
		errs <- r.Report(c, nil) // holds the only slot
		<-blocker
	}()
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 3; i++ {
		res := r.Report(ctx, nil)
		if res.OK || res.Failure != model.FailEndpointRejected {
			t.Fatalf("in-flight overload #%d = %+v", i, res)
		}
	}
	close(blocker)
	<-errs

	if r.RequestCount() != 6+1+1+3 {
		t.Fatalf("request count=%d", r.RequestCount())
	}
	fc := r.FailureCounts()
	if fc[model.FailEndpointRejected] < 3 {
		t.Fatalf("rejected count=%v", fc)
	}
}
