package metrics

import (
	"sync"
	"testing"
)

func TestCountersAndGauges(t *testing.T) {
	c := New()
	c.IncIngested(10)
	c.IncAccepted(7)
	c.IncRejected(3)
	c.IncSampledIn(7)
	c.IncExportSuccesses(6)
	c.IncDLQEnqueued(1)
	if d := c.AddQueueDepth(4); d != 4 {
		t.Fatalf("gauge up = %d", d)
	}
	if d := c.AddQueueDepth(-1); d != 3 {
		t.Fatalf("gauge down = %d", d)
	}

	s := c.Snapshot()
	if s.Ingested != 10 || s.Accepted != 7 || s.Rejected != 3 || s.QueueDepth != 3 || s.DLQEnqueued != 1 {
		t.Fatalf("snapshot wrong: %+v", s)
	}
}

func TestConcurrentCounterIncrements(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				c.IncAccepted(1)
				c.AddQueueDepth(1)
			}
		}()
	}
	wg.Wait()
	if c.Snapshot().Accepted != 32000 || c.Snapshot().QueueDepth != 32000 {
		t.Fatalf("lost increments: %+v", c.Snapshot())
	}
}
