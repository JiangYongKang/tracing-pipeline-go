// Package retention provides a bounded runtime-state store with a hard
// capacity cap, an optional TTL window, deterministic eviction and a
// bounded eviction journal used to classify revisits after reclamation.
//
// The store keeps long-running ingestion pipelines bounded: the live set
// never exceeds MaxEntries, entries idle longer than TTL are reclaimed,
// and reclamation is deterministic (coldest-first by last touch), so the
// same event sequence always produces the same retained set. Reclaiming
// is amortized O(1) on the write path and never blocks writers on
// unbounded work.
package retention

import (
	"container/list"
	"sync"
	"time"
)

// sweepBatch caps how many expired entries one write reclaims, so a
// single Put after a long idle period cannot stall the ingress path.
// Leftover expired entries are still reclaimed deterministically: they
// are the coldest, so capacity eviction removes them first.
const sweepBatch = 256

// Reason tells why an entry was reclaimed.
type Reason int

const (
	// ReasonCapacity: the store was full and the coldest entry was evicted.
	ReasonCapacity Reason = iota
	// ReasonExpired: the entry was untouched longer than TTL.
	ReasonExpired
)

// String renders the reclamation reason for logs and metrics.
func (r Reason) String() string {
	switch r {
	case ReasonCapacity:
		return "capacity"
	case ReasonExpired:
		return "expired"
	default:
		return "unknown"
	}
}

// Config bounds one runtime-state store. The combination of MaxEntries
// and TTL gives a hard upper bound on retained state: the live set never
// exceeds MaxEntries entries, and entries untouched for longer than TTL
// are reclaimed. The eviction journal is separately bounded by
// JournalCapacity, so revisit classification memory is bounded too.
type Config struct {
	MaxEntries      int           // hard cap on live entries
	TTL             time.Duration // max idle retention window; <=0 disables time expiry
	JournalCapacity int           // bounded ring of recently evicted keys
}

// DefaultConfig returns bounded defaults so a long-running process keeps
// a hard state upper bound without any configuration.
func DefaultConfig() Config {
	return Config{
		MaxEntries:      32768,
		TTL:             10 * time.Minute,
		JournalCapacity: 32768,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.MaxEntries <= 0 {
		c.MaxEntries = d.MaxEntries
	}
	if c.JournalCapacity < 0 {
		c.JournalCapacity = 0
	}
	return c
}

// Stats is a point-in-time, externally queryable view of one store.
type Stats struct {
	Retained     int // live entries right now (after sweeping expired)
	Capacity     int // configured hard cap
	EvictedTotal int64
	ExpiredTotal int64
	JournalLen   int
}

// entry is one live key/value pair with its last-touch timestamp.
type entry[K comparable, V any] struct {
	key   K
	val   V
	touch time.Time
}

// eviction is one reclaimed entry, reported to the onEvict hook after
// the store lock is released.
type eviction[K comparable, V any] struct {
	key    K
	val    V
	reason Reason
}

// Store is a bounded, goroutine-safe key/value store with deterministic
// reclamation. Construct with NewStore.
type Store[K comparable, V any] struct {
	mu      sync.Mutex
	cfg     Config
	now     func() time.Time
	onEvict func(K, V, Reason)

	items map[K]*list.Element // key -> *entry, ordered via order
	order *list.List          // front = coldest (least recently touched)

	journal map[K]struct{} // bounded set of recently evicted keys
	jorder  []K            // FIFO order of journal keys

	evictedTotal int64
	expiredTotal int64
}

// NewStore builds a bounded store. now may be nil (uses time.Now); tests
// inject a fake clock for deterministic TTL behavior. onEvict may be nil;
// it is invoked after the store lock is released, once per reclaimed
// entry, so reclamation stays observable without blocking writers.
func NewStore[K comparable, V any](cfg Config, now func() time.Time, onEvict func(K, V, Reason)) *Store[K, V] {
	if now == nil {
		now = time.Now
	}
	return &Store[K, V]{
		cfg:     cfg.withDefaults(),
		now:     now,
		onEvict: onEvict,
		items:   make(map[K]*list.Element),
		order:   list.New(),
		journal: make(map[K]struct{}),
	}
}

// Get returns the live value for k, refreshing its retention window. An
// expired entry is reclaimed and reported as absent.
func (s *Store[K, V]) Get(k K) (V, bool) {
	var zero V
	s.mu.Lock()
	el, ok := s.items[k]
	if !ok {
		s.mu.Unlock()
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if s.expiredLocked(e, s.now()) {
		evicted := s.removeLocked(el, ReasonExpired, nil)
		s.mu.Unlock()
		s.fire(evicted)
		return zero, false
	}
	e.touch = s.now()
	s.order.MoveToBack(el)
	s.mu.Unlock()
	return e.val, true
}

// Put inserts or refreshes k. It may reclaim expired entries and, when
// full, evicts the coldest live entries so the hard cap always holds.
// Reclamation per call is bounded (sweepBatch expired + enough coldest
// evictions to fit one new entry), so writers never stall on unbounded
// work.
func (s *Store[K, V]) Put(k K, v V) {
	var evicted []eviction[K, V]
	s.mu.Lock()
	now := s.now()
	if el, ok := s.items[k]; ok {
		e := el.Value.(*entry[K, V])
		e.val = v
		e.touch = now
		s.order.MoveToBack(el)
		s.mu.Unlock()
		return
	}
	evicted = s.sweepExpiredLocked(now, sweepBatch, evicted)
	for len(s.items) >= s.cfg.MaxEntries {
		front := s.order.Front()
		if front == nil {
			break
		}
		evicted = s.removeLocked(front, ReasonCapacity, evicted)
	}
	s.items[k] = s.order.PushBack(&entry[K, V]{key: k, val: v, touch: now})
	s.mu.Unlock()
	s.fire(evicted)
}

// WasEvicted reports whether k was reclaimed recently enough to still be
// remembered by the bounded eviction journal. It does not mutate state.
func (s *Store[K, V]) WasEvicted(k K) bool {
	s.mu.Lock()
	_, ok := s.journal[k]
	s.mu.Unlock()
	return ok
}

// Len returns the number of live entries, sweeping expired ones first so
// the reported retention is accurate. This is a query path, not the
// write path, so a full sweep is acceptable here.
func (s *Store[K, V]) Len() int {
	s.mu.Lock()
	evicted := s.sweepExpiredLocked(s.now(), 0, nil)
	n := len(s.items)
	s.mu.Unlock()
	s.fire(evicted)
	return n
}

// Stats returns an externally queryable snapshot: current retention,
// configured cap and cumulative reclamation counters.
func (s *Store[K, V]) Stats() Stats {
	s.mu.Lock()
	evicted := s.sweepExpiredLocked(s.now(), 0, nil)
	st := Stats{
		Retained:     len(s.items),
		Capacity:     s.cfg.MaxEntries,
		EvictedTotal: s.evictedTotal,
		ExpiredTotal: s.expiredTotal,
		JournalLen:   len(s.journal),
	}
	s.mu.Unlock()
	s.fire(evicted)
	return st
}

// expiredLocked reports whether e is past the TTL window at now.
func (s *Store[K, V]) expiredLocked(e *entry[K, V], now time.Time) bool {
	return s.cfg.TTL > 0 && now.Sub(e.touch) >= s.cfg.TTL
}

// sweepExpiredLocked reclaims up to limit expired entries from the
// coldest end (limit <= 0 means no limit). Because order is kept
// coldest-first, it stops at the first live entry.
func (s *Store[K, V]) sweepExpiredLocked(now time.Time, limit int, evicted []eviction[K, V]) []eviction[K, V] {
	if s.cfg.TTL <= 0 {
		return evicted
	}
	for limit <= 0 || len(evicted) < limit {
		front := s.order.Front()
		if front == nil {
			break
		}
		e := front.Value.(*entry[K, V])
		if !s.expiredLocked(e, now) {
			break
		}
		evicted = s.removeLocked(front, ReasonExpired, evicted)
	}
	return evicted
}

// removeLocked deletes one element from the live set, records it in the
// bounded journal and returns the eviction record appended to evicted.
func (s *Store[K, V]) removeLocked(el *list.Element, reason Reason, evicted []eviction[K, V]) []eviction[K, V] {
	e := el.Value.(*entry[K, V])
	delete(s.items, e.key)
	s.order.Remove(el)

	if s.cfg.JournalCapacity > 0 {
		if _, seen := s.journal[e.key]; !seen {
			for len(s.jorder) >= s.cfg.JournalCapacity {
				old := s.jorder[0]
				s.jorder = s.jorder[1:]
				delete(s.journal, old)
			}
			s.jorder = append(s.jorder, e.key)
			s.journal[e.key] = struct{}{}
		}
	}
	s.evictedTotal++
	if reason == ReasonExpired {
		s.expiredTotal++
	}
	return append(evicted, eviction[K, V]{key: e.key, val: e.val, reason: reason})
}

// fire invokes the eviction hook outside the store lock.
func (s *Store[K, V]) fire(evicted []eviction[K, V]) {
	if s.onEvict == nil {
		return
	}
	for _, ev := range evicted {
		s.onEvict(ev.key, ev.val, ev.reason)
	}
}
