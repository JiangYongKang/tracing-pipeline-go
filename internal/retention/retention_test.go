package retention

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{MaxEntries: 4, TTL: time.Minute, JournalCapacity: 8}
}

// Capacity eviction is deterministic: the coldest (least recently
// touched) entry goes first, the hard cap always holds, and evicted keys
// land in the bounded journal.
func TestCapacityEvictionColdestFirst(t *testing.T) {
	var log []string
	s := NewStore[int, int](testConfig(), nil, func(k, v int, r Reason) {
		log = append(log, fmt.Sprintf("key=%d reason=%s", k, r))
	})
	for i := 1; i <= 4; i++ {
		s.Put(i, i*10)
	}
	// Touch 1 so 2 becomes the coldest.
	if _, ok := s.Get(1); !ok {
		t.Fatal("1 must be live")
	}
	s.Put(5, 50) // evicts 2
	s.Put(6, 60) // evicts 3

	if got := s.Len(); got != 4 {
		t.Fatalf("retained=%d want cap 4", got)
	}
	if _, ok := s.Get(2); ok {
		t.Fatal("2 should have been evicted (coldest)")
	}
	if _, ok := s.Get(3); ok {
		t.Fatal("3 should have been evicted")
	}
	if !s.WasEvicted(2) || !s.WasEvicted(3) {
		t.Fatal("evicted keys must be remembered by the journal")
	}
	if s.WasEvicted(1) {
		t.Fatal("live key must not be in the journal")
	}
	st := s.Stats()
	if st.EvictedTotal != 2 || st.Capacity != 4 || st.Retained != 4 {
		t.Fatalf("stats=%+v", st)
	}
	if len(log) != 2 || log[0] != "key=2 reason=capacity" || log[1] != "key=3 reason=capacity" {
		t.Fatalf("eviction log=%v", log)
	}
}

// TTL: entries idle past the window are reclaimed on access/insert and
// reported as expired; the fake clock makes it deterministic.
func TestTTLExpiry(t *testing.T) {
	now := time.Unix(1_000, 0)
	clock := func() time.Time { return now }
	s := NewStore[int, int](testConfig(), clock, nil)

	s.Put(1, 10)
	s.Put(2, 20)
	now = now.Add(30 * time.Second)
	s.Put(3, 30)                    // refresh only 3; 1 and 2 idle since t0
	now = now.Add(31 * time.Second) // 1,2 idle 61s > TTL; 3 idle 31s

	if _, ok := s.Get(1); ok {
		t.Fatal("1 must be expired")
	}
	if !s.WasEvicted(1) {
		t.Fatal("expired key must enter the journal")
	}
	if _, ok := s.Get(3); !ok {
		t.Fatal("3 must still be live")
	}
	// 2 is expired but not yet swept; Len/Stats sweep it.
	if got := s.Len(); got != 1 {
		t.Fatalf("retained=%d want 1 (only 3 live)", got)
	}
	st := s.Stats()
	if st.ExpiredTotal != 2 {
		t.Fatalf("expired=%d want 2 stats=%+v", st.ExpiredTotal, st)
	}
}

// The journal is bounded: once full, the oldest remembered key is
// forgotten (FIFO), so revisit detection memory has a hard cap too.
func TestJournalBounded(t *testing.T) {
	cfg := Config{MaxEntries: 2, TTL: 0, JournalCapacity: 3}
	s := NewStore[int, int](cfg, nil, nil)
	for i := 1; i <= 6; i++ {
		s.Put(i, i) // evicts 1..4
	}
	if s.WasEvicted(1) {
		t.Fatal("oldest evicted key must fall out of the bounded journal")
	}
	if !s.WasEvicted(4) {
		t.Fatal("recently evicted key must be in the journal")
	}
	st := s.Stats()
	if st.JournalLen > 3 {
		t.Fatalf("journal len=%d exceeds cap 3", st.JournalLen)
	}
	if st.EvictedTotal != 4 {
		t.Fatalf("evicted=%d want 4", st.EvictedTotal)
	}
}

// Concurrent writers/readers with continuous reclamation: the hard cap
// holds at all times and counters stay consistent. Run with -race.
func TestConcurrentWritesWithEviction(t *testing.T) {
	cfg := Config{MaxEntries: 128, TTL: 0, JournalCapacity: 128}
	s := NewStore[int, int](cfg, nil, nil)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := base*2000 + i
				s.Put(k, k)
				s.Get(k - 1)
				if i%97 == 0 {
					s.WasEvicted(k - 97)
				}
			}
		}(w)
	}
	wg.Wait()
	st := s.Stats()
	if st.Retained > 128 {
		t.Fatalf("retained=%d exceeds cap 128", st.Retained)
	}
	if st.EvictedTotal == 0 {
		t.Fatal("expected continuous eviction under 16k unique keys")
	}
	if st.JournalLen > 128 {
		t.Fatalf("journal len=%d exceeds cap", st.JournalLen)
	}
	t.Logf("retained=%d evicted=%d journal=%d", st.Retained, st.EvictedTotal, st.JournalLen)
}

// Default config must satisfy the hard-bound requirement out of the box.
func TestDefaultConfigBounded(t *testing.T) {
	d := DefaultConfig()
	if d.MaxEntries <= 0 || d.TTL <= 0 || d.JournalCapacity <= 0 {
		t.Fatalf("default config must be bounded: %+v", d)
	}
	s := NewStore[int, int](d, nil, nil)
	for i := 0; i < d.MaxEntries+100; i++ {
		s.Put(i, i)
	}
	if got := s.Len(); got > d.MaxEntries {
		t.Fatalf("retained=%d exceeds default cap %d", got, d.MaxEntries)
	}
}
