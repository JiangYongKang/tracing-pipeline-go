package sampler

import (
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

func mkSpan(tid model.TraceID, sid model.SpanID, parent string) *model.Span {
	s := &model.Span{TraceID: tid, SpanID: sid}
	if parent != "" {
		pid, _ := model.ParseSpanID(parent)
		s.Parent = pid
		s.HasParent = true
	}
	return s
}

func trace(t *testing.T, n int) model.TraceID {
	t.Helper()
	id, err := model.ParseTraceID(fmt.Sprintf("%032x", n))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func spanID(t *testing.T, n int) model.SpanID {
	t.Helper()
	id, err := model.ParseSpanID(fmt.Sprintf("%016x", n))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Every span of one trace must agree, whether parents or children arrive
// first.
func TestParentChildConsistency(t *testing.T) {
	for _, ratio := range []float64{0.3, 0.5, 0.8} {
		s := New(ratio)
		tid := trace(t, 1)
		root := mkSpan(tid, spanID(t, 1), "")
		c1 := mkSpan(tid, spanID(t, 2), fmt.Sprintf("%016x", 1))
		c2 := mkSpan(tid, spanID(t, 3), fmt.Sprintf("%016x", 2))

		// Child arrives before root.
		dC2 := s.Decide(c2)
		dC1 := s.Decide(c1)
		dRoot := s.Decide(root)
		if dRoot == model.DecisionUnknown || dRoot != dC1 || dC1 != dC2 {
			t.Fatalf("ratio %.1f: decisions disagree root=%s c1=%s c2=%s", ratio, dRoot, dC1, dC2)
		}
	}
}

// A ratio change after a trace started must never flip its frozen verdict.
func TestRatioChangeCannotFlipStartedTrace(t *testing.T) {
	s := New(1.0)
	tid := trace(t, 42)
	root := mkSpan(tid, spanID(t, 10), "")
	before := s.Decide(root)
	if before != model.DecisionSample {
		t.Fatalf("ratio 1.0 => %s", before)
	}
	// Flip ratio to 0; new traces drop, but trace 42 stays sampled.
	s.SetRatio(0.0)
	child := mkSpan(tid, spanID(t, 11), fmt.Sprintf("%016x", 10))
	if got := s.Decide(child); got != model.DecisionSample {
		t.Fatalf("started trace flipped to %s", got)
	}
	other := trace(t, 43)
	if got := s.Decide(mkSpan(other, spanID(t, 12), "")); got != model.DecisionDrop {
		t.Fatalf("new trace under ratio 0 = %s, want drop", got)
	}
	snap := s.Snapshot()
	if snap.Sampled != 1 || snap.Dropped != 1 {
		t.Fatalf("snapshot=%+v", snap)
	}
}

func TestDeterministicAtFixedRatio(t *testing.T) {
	a := New(0.5)
	b := New(0.5)
	for i := 1; i <= 500; i++ {
		tid := trace(t, i)
		if a.Decide(mkSpan(tid, spanID(t, i), "")) != b.Decide(mkSpan(tid, spanID(t, i), "")) {
			t.Fatalf("trace %d decided differently across samplers", i)
		}
	}
}

func TestRatioBounds(t *testing.T) {
	if s := New(0.0); s.Snapshot().Ratio != 0 {
		t.Fatal("ratio 0 not honored")
	}
	if s := New(1.0); s.Snapshot().Ratio != 1 {
		t.Fatal("ratio 1 not honored")
	}
	s := New(2.0)
	if s.Snapshot().Ratio != 1 {
		t.Fatal("ratio above 1 should clamp to 1")
	}
	s.SetRatio(-1)
	if s.Snapshot().Ratio != 0 {
		t.Fatal("negative ratio should clamp to 0")
	}
}

// Under ratio 0 and 1, whole traces behave wholesale.
func TestAllOrNothing(t *testing.T) {
	s := New(0.0)
	tid := trace(t, 7)
	for i := 1; i <= 5; i++ {
		var parent string
		if i > 1 {
			parent = fmt.Sprintf("%016x", i-1)
		}
		if d := s.Decide(mkSpan(tid, spanID(t, i), parent)); d != model.DecisionDrop {
			t.Fatalf("ratio0 span %d => %s", i, d)
		}
	}
	s = New(1.0)
	for i := 1; i <= 5; i++ {
		var parent string
		if i > 1 {
			parent = fmt.Sprintf("%016x", i-1)
		}
		if d := s.Decide(mkSpan(tid, spanID(t, i+100), parent)); d != model.DecisionSample {
			t.Fatalf("ratio1 span %d => %s", i, d)
		}
	}
}

// Concurrent decisions on the same trace must always agree.
func TestConcurrentSameTraceConsistency(t *testing.T) {
	s := New(0.5)
	tid := trace(t, 99)
	const n = 200
	var wg sync.WaitGroup
	results := make([]model.Decision, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			parent := ""
			if i > 0 {
				parent = fmt.Sprintf("%016x", i) // parent may not have arrived
			}
			results[i] = s.Decide(mkSpan(tid, spanID(t, i+1), parent))
		}(i)
	}
	wg.Wait()
	first := results[0]
	for i, d := range results {
		if d != first {
			t.Fatalf("span %d decided %s, want %s", i, d, first)
		}
	}
}
