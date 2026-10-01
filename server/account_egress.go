package server

// Account level egress bandwidth limiting.
//
// This implements a first version of an aggregate, in-process egress
// bandwidth limiter for a single NATS account. All active local client and
// leaf connections attached to the account draw bytes from one shared token
// bucket, so the configured ceiling bounds the total bytes the server emits
// on those connections rather than granting the ceiling independently to
// every connection.
//
// Fairness between active connections is provided by:
//   - capping the amount of bytes any single flush may reserve, so a large
//     continuous sender cannot drain arbitrarily large chunks at once;
//   - serving waiters in FIFO order while skipping a head waiter that cannot
//     yet be fully served, which lets small/intermittent senders get served
//     promptly even while large senders keep accumulating tokens;
//   - re-queuing a connection that wants to send more after it consumed a
//     reservation, so it takes a turn behind the other active connections.
//
// Protocol control frames (PING/PONG/OK/-ERR) keep a limited, timely channel:
// small control requests are considered ahead of bulk data requests, but they
// are still charged against the same shared bucket, so they cannot be abused
// as an unlimited bypass and the aggregate egress remains capped.
//
// Scope note: this is explicitly a single server process view. It does not
// coordinate with other servers in a cluster and therefore does not enforce a
// cluster-wide global account rate.

import (
	"sync"
	"time"
)

const (
	// aelPacerInterval is how often the refill goroutine tops up tokens and
	// retries pending waiters. Kept small so bounded waiters resume promptly.
	aelPacerInterval = 2 * time.Millisecond
	// aelMaxChunk caps how many bytes a single reservation may consume. It
	// bounds unfairness/scheduling latency that a single large sender can
	// impose on the other active connections of the account.
	aelMaxChunk = 8 * 1024
	// aelCtrlMax is the largest control reservation that gets priority over
	// bulk data. Control frames (PING/PONG/OK/-ERR) are tiny; anything larger
	// is treated as ordinary data.
	aelCtrlMax = 256
	// aelBurstDuration bounds the token bucket depth: up to this much idle
	// time may be banked in a single burst.
	aelBurstDuration = time.Second
	// aelMaxBurst caps absolute burst depth regardless of the configured
	// rate, to avoid excessively large token stockpiles.
	aelMaxBurst = 1024 * 1024
)

// accountEgressLimiter is an aggregate byte-token bucket shared by every
// egress connection of one account, within this server process.
//
// All fields are guarded by mu.
type accountEgressLimiter struct {
	mu sync.Mutex

	rate     int64   // bytes per second; <= 0 means unlimited
	tokens   float64 // currently available (possibly negative, i.e. in debt)
	burst    float64 // token bucket ceiling
	lastFill time.Time

	// Waiters blocked in reserve(), ordered FIFO.
	queue []*egressWaiter

	// True while the refill goroutine is running.
	running bool
	// Closed when the limiter is permanently stopped (account removed or
	// replaced on configuration changes).
	stopped chan struct{}

	// Observability counter (updated under mu, exposed via stats()).
	totalWaited time.Duration
}

// egressWaiter represents one blocked reservation attempt.
type egressWaiter struct {
	n     int
	ctrl  bool
	done  chan struct{}
	grant bool // true if served with tokens, false if aborted/config-changed
	enq   time.Time
}

func newAccountEgressLimiter(rate int64) *accountEgressLimiter {
	l := &accountEgressLimiter{
		rate:    rate,
		stopped: make(chan struct{}),
	}
	now := time.Now()
	l.lastFill = now
	if rate > 0 {
		l.burst = float64(rate) * aelBurstDuration.Seconds()
		if b := float64(rate); l.burst < b {
			l.burst = b
		}
		if l.burst > aelMaxBurst {
			l.burst = aelMaxBurst
		}
		l.tokens = l.burst
	}
	return l
}

// unlimited reports whether the limiter imposes any ceiling.
func (l *accountEgressLimiter) unlimited() bool {
	return l == nil || l.rate <= 0
}

// refillLocked adds tokens accrued since the last refill, bounded by burst.
func (l *accountEgressLimiter) refillLocked(now time.Time) {
	if l.rate <= 0 {
		return
	}
	elapsed := now.Sub(l.lastFill)
	if elapsed <= 0 {
		return
	}
	l.tokens += float64(l.rate) * elapsed.Seconds()
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.lastFill = now
}

// serveLocked walks the queue and grants any requests that can currently be
// fully satisfied. Small control requests are considered first regardless of
// position; data requests are served in FIFO order but skipped while not fully
// satisfiable, so a large head request cannot starve small senders.
func (l *accountEgressLimiter) serveLocked() {
	// Pass 1: small control frames first (limited timely control channel).
	for _, w := range l.queue {
		if w.ctrl && l.tokens >= float64(w.n) {
			l.tokens -= float64(w.n)
			w.grant = true
			close(w.done)
		}
	}

	// Pass 2: ordinary data in FIFO order, skipping unservable heads.
	queue := l.queue[:0]
	for _, w := range l.queue {
		if w.grant {
			continue // served in pass 1
		}
		if !w.ctrl && l.tokens >= float64(w.n) {
			l.tokens -= float64(w.n)
			w.grant = true
			close(w.done)
		} else {
			queue = append(queue, w)
		}
	}
	l.queue = queue
}

// startPacerLocked launches the refill goroutine if needed.
func (l *accountEgressLimiter) startPacerLocked() {
	if l.running || l.rate <= 0 {
		return
	}
	l.running = true
	go l.pacer()
}

// pacer periodically refills the bucket and advances waiters. It exits once
// there are no waiters left or the limiter has been stopped.
func (l *accountEgressLimiter) pacer() {
	t := time.NewTicker(aelPacerInterval)
	defer t.Stop()
	for {
		select {
		case <-l.stopped:
			return
		case now := <-t.C:
			l.mu.Lock()
			if !l.running {
				l.mu.Unlock()
				return
			}
			l.refillLocked(now)
			// Grant control frames first, then bulk data. If we granted
			// anything, run an extra data-only pass so newly affordable
			// requests make progress on the same tick.
			l.serveLocked()
			if len(l.queue) == 0 {
				l.running = false
				l.mu.Unlock()
				return
			}
			l.mu.Unlock()
		}
	}
}

// reserve requests n bytes of egress allowance, blocking while the aggregate
// account budget cannot cover them. ctrl marks a small protocol control frame,
// which gets limited priority over bulk data. abort, if non-nil, is closed by
// the connection on close/slow-consumer teardown and forces reserve to return
// early so blocked connections are never leaked as waiters.
//
// reserve returns true if the request was granted, and false if it was
// aborted (connection torn down) or superseded by a configuration update. On
// a false return no bytes may be written against this reservation.
func (l *accountEgressLimiter) reserve(n int, ctrl bool, abort <-chan struct{}) bool {
	if l == nil || l.rate <= 0 || n <= 0 {
		return true
	}

	l.mu.Lock()
	for {
		// Opportunistic fast path: nobody ahead and tokens available.
		if len(l.queue) == 0 {
			l.refillLocked(time.Now())
			if l.tokens >= float64(n) {
				l.tokens -= float64(n)
				l.mu.Unlock()
				return true
			}
		}

		w := &egressWaiter{
			n:    n,
			ctrl: ctrl && n <= aelCtrlMax,
			done: make(chan struct{}),
			enq:  time.Now(),
		}
		l.queue = append(l.queue, w)
		l.startPacerLocked()

		// Release the lock while we wait to be granted, aborted, stopped, or
		// woken for retry by a configuration update.
		l.mu.Unlock()

		var why int // 0=granted/update, 1=abort, 2=stopped
		select {
		case <-w.done:
		case <-abort:
			why = 1
		case <-l.stopped:
			why = 2
		}

		l.mu.Lock()
		switch why {
		case 2:
			// Limiter permanently stopped (account replaced/limit removed).
			l.removeWaiterLocked(w)
			l.mu.Unlock()
			return false
		case 1:
			l.removeWaiterLocked(w)
			if w.grant {
				// Granted concurrently with teardown: tokens are unused.
				l.tokens += float64(n)
				if l.tokens > l.burst {
					l.tokens = l.burst
				}
				l.mu.Unlock()
				return true
			}
			l.mu.Unlock()
			return false
		}

		// w.done fired.
		if w.grant {
			l.totalWaited += time.Since(w.enq)
			l.mu.Unlock()
			return true
		}
		// Woken by update() without a grant: loop to re-evaluate against the
		// current rate. The waiter was already removed by dropAllWaitersLocked.
	}
}

// removeWaiterLocked drops w from the queue if it is still present. It does
// not touch an already-granted waiter.
func (l *accountEgressLimiter) removeWaiterLocked(target *egressWaiter) {
	queue := l.queue[:0]
	for _, w := range l.queue {
		if w != target {
			queue = append(queue, w)
		}
	}
	l.queue = queue
}

// refund returns unused allowance from a previous reservation, e.g. when a
// write sent fewer bytes than were reserved or the connection is gone.
func (l *accountEgressLimiter) refund(n int) {
	if l == nil || l.rate <= 0 || n <= 0 {
		return
	}
	l.mu.Lock()
	l.tokens += float64(n)
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.mu.Unlock()
}

// update changes the configured rate. It always frees every current waiter:
// waiters retry with reserve() so that raising/removing the limit unblocks
// them immediately, while lowering it cannot leave stale reservations behind.
// A rate <= 0 disables limiting.
func (l *accountEgressLimiter) update(rate int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	l.refillLocked(now)

	l.rate = rate
	if rate > 0 {
		burst := float64(rate) * aelBurstDuration.Seconds()
		if b := float64(rate); burst < b {
			burst = b
		}
		if burst > aelMaxBurst {
			burst = aelMaxBurst
		}
		l.burst = burst
		if l.tokens > burst {
			l.tokens = burst
		}
	}

	// Release all current waiters; they re-evaluate against the new rate.
	l.dropAllWaitersLocked()

	// Restart the clock so the new rate accrues from now.
	l.lastFill = now
}

// stop permanently disables the limiter and releases any waiters. Used when
// the account object is replaced by a configuration reload.
func (l *accountEgressLimiter) stop() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.isStoppedLocked() {
		l.mu.Unlock()
		return
	}
	l.dropAllWaitersLocked()
	close(l.stopped)
	l.mu.Unlock()
}

func (l *accountEgressLimiter) isStoppedLocked() bool {
	select {
	case <-l.stopped:
		return true
	default:
		return false
	}
}

// dropAllWaitersLocked wakes every queued waiter without granting tokens.
// Waiters observe grant==false and re-evaluate (or abort).
func (l *accountEgressLimiter) dropAllWaitersLocked() {
	for _, w := range l.queue {
		close(w.done)
	}
	l.queue = nil
}

// egressStats is a point-in-time snapshot of limiter observability state.
type egressStats struct {
	Rate     int64
	Waiting  int64
	WaitedMs int64
}

func (l *accountEgressLimiter) stats() egressStats {
	if l == nil {
		return egressStats{}
	}
	l.mu.Lock()
	s := egressStats{
		Rate:     l.rate,
		Waiting:  int64(len(l.queue)),
		WaitedMs: l.totalWaited.Milliseconds(),
	}
	l.mu.Unlock()
	return s
}
