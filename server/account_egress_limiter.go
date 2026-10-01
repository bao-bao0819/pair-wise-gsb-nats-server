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
	"time"
)

// egressBurstMs bounds how many milliseconds of credit the shared data
// bucket may bank ahead of time. It keeps startup/queuing burst modest so
// measured aggregate throughput tracks the configured rate closely even
// over short windows, while still absorbing ordinary scheduling jitter.
const egressBurstMs = 100

// egressMaxGrant is the largest number of bytes a single connection is
// allowed to charge in one turn. It bounds the time an intermittent small
// sender can be blocked behind continuous big senders: even if a big
// connection has megabytes queued, it releases the limiter at least this
// often so other active connections get a fair turn.
const egressMaxGrant = 64 * 1024

// egressControlBurst is the size of the prioritized token bucket reserved
// for protocol control frames (PING/PONG, -ERR, INFO, ...). It is
// deliberately small: control frames get a finite, timely bypass of the
// shared data quota, but a peer cannot turn it into an unlimited side
// channel since sustained control traffic above this rate falls back onto
// the regular account quota.
const egressControlBurst = 8 * 1024

// egressControlMinRate is the floor for the control bucket refill rate.
const egressControlMinRate = 256 // bytes per second

// egressSchedulingGranularity is the minimum timer period driving grants.
const egressSchedulingGranularity = time.Millisecond

// egressMinGrant is the smallest slice of quota handed to a waiting
// connection. Granting in small chunks (rather than only releasing a
// connection once a full egressMaxGrant has accrued) keeps writes flowing
// smoothly even at low configured rates, where accruing a full 64KB turn
// could otherwise take a large fraction of a second.
const egressMinGrant = 2 * 1024

// egressWaiter represents a connection blocked in the write path waiting
// for its fair share of the account's egress quota.
type egressWaiter struct {
	// need is the maximum turn size this connection can use.
	need int64
	// amount is the quota actually reserved once granted fires.
	amount int64
	// granted is closed by the scheduler once at least egressMinGrant (or
	// the remaining need) has been reserved.
	granted chan struct{}
}

// accountEgressLimiter enforces a single process-local aggregate egress
// bandwidth ceiling for all connections of one account.
//
// The quota covers bytes actually written to sockets and is shared: there
// is one bucket for the whole account rather than one per connection.
// Waiting connections are served strictly FIFO in turns of at most
// egressMaxGrant bytes: a big sender that received a turn re-enters behind
// every already-queued peer, so active connections get fair shares and the
// wait of an intermittent sender behind continuous big senders is bounded
// by roughly (#active peers * egressMaxGrant / rate).
//
// A second, small bucket keeps a bounded priority lane for protocol control
// frames so that liveness traffic is not stuck behind bulk data.
//
// This limiter is intentionally scoped to a single server process. It does
// not coordinate a cluster-wide limit.
type accountEgressLimiter struct {
	rate  int64   // bytes per second for the shared data bucket
	burst float64 // token bucket capacity (bytes)

	// Shared data bucket and FIFO waiter queue.
	mu      sync.Mutex
	tokens  float64
	closed  bool
	waiters []*egressWaiter
	tmr     *time.Timer
	tmrSet  bool

	// Bounded priority lane for protocol control frames.
	ctrlRate   float64
	ctrlTokens float64

	now  func() time.Time
	last time.Time

	// grantedData is the cumulative data bytes handed out (test observability).
	grantedData atomic.Int64
}

// newAccountEgressLimiter creates a limiter with the given aggregate rate
// in bytes per second. The data bucket starts with one fair turn banked so
// traffic does not get an artificial delay after configuration.
func newAccountEgressLimiter(bps int64) *accountEgressLimiter {
	burst := float64(bps) * egressBurstMs / 1000
	l := &accountEgressLimiter{
		rate:       bps,
		burst:      burst,
		tokens:     burst,
		ctrlRate:   float64(max(bps/10, egressControlMinRate)),
		ctrlTokens: egressControlBurst,
		now:        time.Now,
	}
	l.last = l.now()
	return l
}

func (l *accountEgressLimiter) refillLocked() {
	now := l.now()
	elapsed := now.Sub(l.last)
	if elapsed <= 0 {
		return
	}
	l.last = now
	l.tokens = min(l.burst, l.tokens+float64(l.rate)*elapsed.Seconds())
	// The control bucket keeps a fixed small ceiling regardless of rate.
	l.ctrlTokens = min(egressControlBurst, l.ctrlTokens+l.ctrlRate*elapsed.Seconds())
}

// tryAcquireControl charges control frames against the bounded priority
// lane. It never blocks: when the lane is empty the caller falls back onto
// the shared data quota, preventing the lane being abused as an unlimited
// bypass. Returns the bytes charged to the control lane.
func (l *accountEgressLimiter) tryAcquireControl(n int64) int64 {
	if n <= 0 {
		return 0
	}
	l.mu.Lock()
	l.refillLocked()
	granted := int64(min(float64(n), l.ctrlTokens))
	l.ctrlTokens -= float64(granted)
	l.mu.Unlock()
	return granted
}

// tryAcquireData charges up to n data bytes that are immediately available,
// never blocking. Used for the final drain of a closing connection, which
// must not wait on the queue and simply sends what it can before teardown.
// Returns 0 if no data tokens are available right now.
func (l *accountEgressLimiter) tryAcquireData(n int64) int64 {
	if n <= 0 {
		return 0
	}
	l.mu.Lock()
	l.refillLocked()
	g := int64(min(float64(n), l.tokens))
	l.tokens -= float64(g)
	if g > 0 {
		l.grantedData.Add(g)
	}
	l.mu.Unlock()
	return g
}

// Acquire blocks until at least egressMinGrant bytes of the account's
// shared data quota (or the remaining need, whichever is smaller) are
// available for this connection, or until done is closed (connection close
// or account configuration change) or the limiter is closed. Returns the
// granted turn size (<= n and <= egressMaxGrant), or 0 if it was canceled
// before receiving any quota.
func (l *accountEgressLimiter) Acquire(done <-chan struct{}, n int64) int64 {
	n = min(n, egressMaxGrant)
	if n <= 0 {
		return 0
	}

	l.mu.Lock()
	for {
		if l.closed {
			l.mu.Unlock()
			return 0
		}
		l.refillLocked()

		// Fast path: no contention and enough tokens for the whole turn.
		if len(l.waiters) == 0 && l.tokens >= egressMinGrant {
			g := int64(min(float64(n), l.tokens))
			l.tokens -= float64(g)
			l.grantedData.Add(g)
			l.mu.Unlock()
			return g
		}

		w := &egressWaiter{need: n, granted: make(chan struct{})}
		l.waiters = append(l.waiters, w)
		l.scheduleLocked()
		l.mu.Unlock()

		select {
		case <-done:
			l.mu.Lock()
			for i, qw := range l.waiters {
				if qw == w {
					l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
					// A head-of-queue waiter went away (connection closed
					// or config changed); re-arm so peers behind it are
					// not stuck waiting on its old deadline.
					if l.tmrSet && l.tmr != nil {
						l.tmr.Stop()
						l.tmrSet = false
					}
					l.scheduleLocked()
					l.mu.Unlock()
					return 0
				}
			}
			// Reserved and popped concurrently; report whatever quota was
			// charged so the caller can use it.
			l.mu.Unlock()
			return w.amount
		case <-w.granted:
			l.mu.Lock()
			if l.closed {
				l.mu.Unlock()
				return 0
			}
			g := w.amount
			l.mu.Unlock()
			return g
		}
	}
}

// scheduleLocked serves any waiter already affordable, then arms a
// short-period self-rescheduling timer while waiters remain. Using a fixed
// tick (rather than one exact deadline per head waiter) makes grants robust
// to waiters joining, leaving, or being canceled: the tick simply
// re-evaluates the queue. The limiter keeps no idle timer once the queue
// drains.
func (l *accountEgressLimiter) scheduleLocked() {
	l.refillLocked()
	for l.grantLocked() {
	}
	if len(l.waiters) == 0 || l.tmrSet || l.closed {
		return
	}
	l.tmrSet = true
	if l.tmr == nil {
		l.tmr = time.AfterFunc(egressSchedulingGranularity, l.onTimer)
	} else {
		l.tmr.Reset(egressSchedulingGranularity)
	}
}

func (l *accountEgressLimiter) onTimer() {
	l.mu.Lock()
	l.tmrSet = false
	if !l.closed {
		l.scheduleLocked()
	}
	l.mu.Unlock()
}

// grantLocked serves every waiter from the head whose full turn is already
// affordable. Returns true if at least one waiter was released.
func (l *accountEgressLimiter) grantLocked() bool {
	served := false
	for len(l.waiters) > 0 {
		w := l.waiters[0]
		g := int64(min(float64(w.need), l.tokens))
		if g < egressMinGrant && g < w.need {
			break
		}
		if g <= 0 {
			break
		}
		l.tokens -= float64(g)
		l.grantedData.Add(g)
		w.amount = g
		l.waiters = l.waiters[1:]
		close(w.granted)
		served = true
	}
	return served
}

// Close releases every blocked waiter. Called when the account limit is
// removed or replaced by a configuration update.
func (l *accountEgressLimiter) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	// Stop the refill timer before releasing waiters so no concurrent
	// grant can race the shutdown.
	if l.tmrSet && l.tmr != nil {
		l.tmr.Stop()
		l.tmrSet = false
	}
	waiters := l.waiters
	l.waiters = nil
	for _, w := range waiters {
		close(w.granted)
	}
	l.mu.Unlock()
}
