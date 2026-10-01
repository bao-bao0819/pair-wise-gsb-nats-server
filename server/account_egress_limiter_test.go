// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// controllableClock lets tests advance the limiter's notion of time.
func newTestLimiter(bps int64, start time.Time) *accountEgressLimiter {
	l := newAccountEgressLimiter(bps)
	l.now = func() time.Time { return start }
	l.last = start
	l.tokens = float64(min(bps, egressMaxGrant))
	l.ctrlTokens = egressControlBurst
	return l
}

func TestEgressLimiterSaturatesAtAggregateRate(t *testing.T) {
	l := newAccountEgressLimiter(100_000)
	done := make(chan struct{})

	// Several hot connections draw from the single shared bucket.
	const conns = 4
	var written [conns]int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				n := l.Acquire(done, egressMaxGrant)
				if n <= 0 {
					return
				}
				atomic.AddInt64(&written[idx], n)
			}
		}(i)
	}

	time.Sleep(500 * time.Millisecond)
	close(done)
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	var total int64
	for i := range written {
		total += atomic.LoadInt64(&written[i])
	}

	rate := float64(total) / elapsed
	// Allow scheduling slack; the key assertion is that the aggregate stays
	// near the single ceiling rather than conns*ceiling.
	if rate > 100_000*1.35 {
		t.Fatalf("aggregate rate %.0f B/s exceeds 100KB/s ceiling (total=%d elapsed=%.3f)", rate, total, elapsed)
	}
	if rate < 100_000*0.6 {
		t.Fatalf("aggregate rate %.0f B/s far below 100KB/s ceiling (total=%d elapsed=%.3f)", rate, total, elapsed)
	}
}

func TestEgressLimiterFairShare(t *testing.T) {
	l := newAccountEgressLimiter(256_000)
	done := make(chan struct{})

	const conns = 4
	var bytes [conns]int64
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				n := l.Acquire(done, egressMaxGrant)
				if n <= 0 {
					return
				}
				atomic.AddInt64(&bytes[idx], n)
			}
		}(i)
	}

	time.Sleep(2 * time.Second)
	close(done)
	wg.Wait()

	var minB, maxB int64 = 1<<62 - 1, 0
	for i := range bytes {
		v := atomic.LoadInt64(&bytes[i])
		if v < minB {
			minB = v
		}
		if v > maxB {
			maxB = v
		}
	}
	// Every active connection must be served (min > 0) and shares must be
	// within a bounded ratio. Initial burst and the 1ms scheduling
	// granularity cause startup skew, so allow a loose ratio over the run.
	if minB == 0 || maxB > minB*3 {
		t.Fatalf("unfair byte distribution min=%d max=%d", minB, maxB)
	}
}

func TestEgressLimiterControlLaneIsBounded(t *testing.T) {
	start := time.Now()
	l := newTestLimiter(10_000, start)

	// Initial burst up to egressControlBurst is free.
	if g := l.tryAcquireControl(egressControlBurst); g != egressControlBurst {
		t.Fatalf("expected full control burst, got %d", g)
	}
	// Once empty it is non-blocking and grants nothing; the excess must
	// fall onto the shared data quota instead of bypassing it.
	if g := l.tryAcquireControl(1024); g != 0 {
		t.Fatalf("expected empty control lane, got %d", g)
	}

	// Advance far enough to refill.
	l.now = func() time.Time { return start.Add(time.Second) }
	// The control lane refills at its own small rate, capped at the burst.
	g := l.tryAcquireControl(egressControlBurst * 2)
	// One second of refill at ctrlRate (max(10000/10,256)=1000 B/s).
	if g != 1000 {
		t.Fatalf("expected bounded refill of 1000 bytes, got %d", g)
	}
}

func TestEgressLimiterCloseReleasesWaiters(t *testing.T) {
	l := newTestLimiter(1_000, time.Now())
	// Start with an empty data bucket so requests must wait.
	l.mu.Lock()
	l.tokens = 0
	l.mu.Unlock()

	const n = 3
	errs := make(chan int64, n)
	for i := 0; i < n; i++ {
		go func() {
			errs <- l.Acquire(make(chan struct{}), egressMaxGrant)
		}()
	}
	// Let the waiters queue.
	time.Sleep(50 * time.Millisecond)

	doneAt := time.Now()
	l.Close()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for i := 0; i < n; i++ {
		select {
		case g := <-errs:
			if g != 0 {
				t.Fatalf("closed limiter should release waiter with 0, got %d", g)
			}
		case <-timer.C:
			t.Fatalf("waiter %d not released by Close", i)
		}
	}
	if d := time.Since(doneAt); d > time.Second {
		t.Fatalf("waiters released too slowly: %v", d)
	}
}

func TestEgressLimiterDoneChannelAbortsWait(t *testing.T) {
	l := newTestLimiter(1_000, time.Now())
	l.mu.Lock()
	l.tokens = 0
	l.mu.Unlock()

	done := make(chan struct{})
	res := make(chan int64, 1)
	go func() { res <- l.Acquire(done, egressMaxGrant) }()
	time.Sleep(50 * time.Millisecond)
	close(done)

	select {
	case g := <-res:
		if g != 0 {
			t.Fatalf("aborted wait should return 0, got %d", g)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not aborted")
	}

	// After an aborted waiter, the next request still queues/serves cleanly.
	l.Close()
}

func TestEgressLimiterRefund(t *testing.T) {
	l := newTestLimiter(100_000, time.Now())
	l.mu.Lock()
	l.tokens = l.burst - 30_000
	l.mu.Unlock()
	tr := egressTurn{l: l, dataN: 30_000}
	tr.refund(30_000, 0)
	// Refund restores capacity up to the burst cap.
	l.mu.Lock()
	tok := l.tokens
	l.mu.Unlock()
	if tok != l.burst {
		t.Fatalf("expected full burst after refund, got %.0f want %.0f", tok, l.burst)
	}
}

func TestEgressLimiterTryAcquireDataNeverBlocks(t *testing.T) {
	l := newTestLimiter(10_000, time.Now())
	l.mu.Lock()
	l.tokens = 1000
	l.mu.Unlock()

	if g := l.tryAcquireData(4096); g != 1000 {
		t.Fatalf("expected immediate partial 1000, got %d", g)
	}
	if g := l.tryAcquireData(4096); g != 0 {
		t.Fatalf("expected 0 when empty, got %d", g)
	}
}
