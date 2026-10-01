package server

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestAccountEgressLimiterAggregates(t *testing.T) {
	// Rate: 100KB/s with up to ~1s burst. A single reservation larger than the
	// burst must block until enough time accrues, proving the shared bucket
	// governs real emission timing.
	l := newAccountEgressLimiter(100 * 1024)

	// Drain most of the burst.
	start := time.Now()
	if !l.reserve(int(l.burst), false, nil) {
		t.Fatalf("initial burst reservation failed")
	}

	// Next chunk must wait for accrual (~8KB at 100KB/s => ~80ms).
	if !l.reserve(aelMaxChunk, false, nil) {
		t.Fatalf("second reservation failed")
	}
	elapsed := time.Since(start)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected to wait for accrual, got %v", elapsed)
	}

	l.stop()
}

func TestAccountEgressLimiterFairShare(t *testing.T) {
	// Two concurrent large senders over a shared low-rate bucket should each
	// get a comparable share of the budget across many chunks. Skipping an
	// unservable head plus FIFO re-queue on each fresh reserve call is what
	// keeps this bounded.
	const rate = 64 * 1024
	l := newAccountEgressLimiter(rate)

	var mu sync.Mutex
	totals := [2]int64{}

	// Burn the initial burst so both start from an empty bucket.
	if !l.reserve(int(l.burst), false, nil) {
		t.Fatalf("burst drain failed")
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			deadline := time.Now().Add(400 * time.Millisecond)
			for time.Now().Before(deadline) {
				if !l.reserve(aelMaxChunk, false, nil) {
					return
				}
				mu.Lock()
				totals[idx] += aelMaxChunk
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	l.stop()

	// Each sender should receive roughly half (allow generous CI slack). The
	// key guarantee is that one sender cannot take the whole rate.
	a, b := totals[0], totals[1]
	if a == 0 || b == 0 {
		t.Fatalf("expected both senders to be served, got %d and %d", a, b)
	}
	ratio := float64(a) / float64(b)
	if ratio < 0.5 || ratio > 2.0 {
		t.Fatalf("expected comparable shares, got %d vs %d (ratio %.2f)", a, b, ratio)
	}
}

func TestAccountEgressLimiterControlPriority(t *testing.T) {
	// When the bucket is empty and a large data waiter is queued, a small
	// control frame should be granted as soon as the few bytes accrue, even
	// though it enqueued later. But control is still charged: it cannot exceed
	// the aggregate rate over time.
	l := newAccountEgressLimiter(8 * 1024)

	// Drain everything.
	if !l.reserve(int(l.burst), false, nil) {
		t.Fatalf("drain failed")
	}

	dataDone := make(chan struct{})
	go func() {
		// Big request that cannot be fully served quickly.
		l.reserve(aelMaxChunk, false, nil)
		close(dataDone)
	}()

	// Let the data waiter enqueue.
	time.Sleep(20 * time.Millisecond)

	ctrlStart := time.Now()
	// PONG frame is a handful of bytes.
	if !l.reserve(8, true, nil) {
		t.Fatalf("control reservation failed")
	}
	ctrlElapsed := time.Since(ctrlStart)

	// The small control frame should be served quickly (a few ms of accrual),
	// well before the 8KB data waiter.
	if ctrlElapsed > 40*time.Millisecond {
		t.Fatalf("control frame was not served promptly: %v", ctrlElapsed)
	}

	select {
	case <-dataDone:
		t.Fatalf("large data waiter should still be blocked")
	default:
	}

	l.stop()
	<-dataDone // aborted by stop
}

func TestAccountEgressLimiterAbortReleasesWaiter(t *testing.T) {
	l := newAccountEgressLimiter(4 * 1024)
	if !l.reserve(int(l.burst), false, nil) {
		t.Fatalf("drain failed")
	}

	abort := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		result <- l.reserve(aelMaxChunk, false, abort)
	}()

	time.Sleep(20 * time.Millisecond)
	close(abort)

	select {
	case granted := <-result:
		if granted {
			t.Fatalf("expected aborted reservation to return false")
		}
	case <-time.After(time.Second):
		t.Fatalf("abort did not release waiter")
	}

	l.mu.Lock()
	queued := len(l.queue)
	l.mu.Unlock()
	if queued != 0 {
		t.Fatalf("expected empty queue after abort, got %d", queued)
	}
	l.stop()
}

func TestAccountEgressLimiterUpdateReleasesWaiters(t *testing.T) {
	l := newAccountEgressLimiter(4 * 1024)
	if !l.reserve(int(l.burst), false, nil) {
		t.Fatalf("drain failed")
	}

	result := make(chan bool, 1)
	go func() {
		// This keeps retrying; raising the limit must unblock it.
		for {
			if l.reserve(aelMaxChunk, false, nil) {
				result <- true
				return
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	l.update(1024 * 1024) // raise ceiling substantially

	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatalf("update did not release waiter")
	}

	// Removing the limit disables gating entirely.
	l.update(0)
	if !l.unlimited() {
		t.Fatalf("expected limiter to be unlimited after update(0)")
	}
	l.stop()
}

func TestAccountConfigureEgress(t *testing.T) {
	a := NewAccount("A")
	if l := a.egressLimiter(); l != nil {
		t.Fatalf("expected no limiter by default")
	}

	a.configureEgress(1234)
	if l := a.egressLimiter(); l == nil || l.rate != 1234 {
		t.Fatalf("expected limiter at 1234, got %v", l)
	}

	// Updating keeps the same limiter object.
	first := a.egressLimiter()
	a.configureEgress(5678)
	if l := a.egressLimiter(); l != first || l.rate != 5678 {
		t.Fatalf("expected same limiter updated to 5678")
	}

	// Disabling tears it down.
	a.configureEgress(0)
	if l := a.egressLimiter(); l != nil {
		t.Fatalf("expected limiter removed when disabled, got %v", l)
	}
}

func TestLeadingControlBytes(t *testing.T) {
	cases := []struct {
		name string
		buf  net.Buffers
		max  int64
		want int64
	}{
		{"pong", net.Buffers{[]byte("PONG\r\nrest")}, 8192, int64(len("PONG\r\n"))},
		{"ping", net.Buffers{[]byte("PING\r\n")}, 8192, int64(len("PING\r\n"))},
		{"ok", net.Buffers{[]byte("+OK\r\n")}, 8192, int64(len("+OK\r\n"))},
		{"err", net.Buffers{[]byte("-ERR 'bad'\r\n")}, 8192, int64(len("-ERR 'bad'\r\n"))},
		{"data", net.Buffers{[]byte("MSG foo 1 5\r\nhello\r\n")}, 8192, 0},
		{"info", net.Buffers{[]byte("INFO {}\r\n")}, 8192, 0},
		{"pong truncated", net.Buffers{[]byte("PONG\r\n")}, 3, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := leadingControlBytes(tc.buf, tc.max); got != tc.want {
				t.Fatalf("leadingControlBytes = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSplitAndDropBufs(t *testing.T) {
	// Two pooled buffers; split in the middle of the second.
	bufs := net.Buffers{[]byte("AAAA"), []byte("BBBB")}
	head := splitBufsHead(bufs, 6)
	if got := string(bytesJoin(head)); got != "AAAABB" {
		t.Fatalf("head = %q, want AAAABB", got)
	}
	// Originals must be untouched (prefix was copied).
	if string(bufs[1]) != "BBBB" {
		t.Fatalf("original boundary buffer mutated: %q", bufs[1])
	}

	// Drop the 6 written bytes from the originals.
	tail := dropFrontBufs(net.Buffers{[]byte("AAAA"), []byte("BBBB")}, 6)
	if got := string(bytesJoin(tail)); got != "BB" {
		t.Fatalf("tail = %q, want BB", got)
	}
}

func bytesJoin(bufs net.Buffers) []byte {
	var out []byte
	for _, b := range bufs {
		out = append(out, b...)
	}
	return out
}
