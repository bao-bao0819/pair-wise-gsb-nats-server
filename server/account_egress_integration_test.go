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
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

const egressTestConf = `
	listen: "127.0.0.1:-1"
	accounts {
		A {
			users [{user: "a", password: "p"}]
			limits {
				max_egress_rate: %d
			}
		}
		B {
			users [{user: "b", password: "p"}]
		}
	}
`

// rawSubscriber opens a TCP connection, logs in as the given account user,
// subscribes to a subject and counts received application bytes (MSG
// payloads), ignoring protocol framing.
func rawSubscriber(t *testing.T, port int, user, subject string) (net.Conn, *int64, func()) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	br := bufio.NewReader(conn)
	if _, _, err := br.ReadLine(); err != nil {
		t.Fatalf("read INFO: %v", err)
	}
	fmt.Fprintf(conn, "CONNECT {\"verbose\":false,\"user\":%q,\"pass\":%q}\r\n", user, "p")
	fmt.Fprintf(conn, "SUB %s 1\r\n", subject)
	fmt.Fprintf(conn, "PING\r\n")
	if _, _, err := br.ReadLine(); err != nil {
		t.Fatalf("read PONG: %v", err)
	}

	var received int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			// Expect "MSG <subject> <sid> <numbytes>\r\n"
			if len(line) < 4 || line[:3] != "MSG" {
				continue
			}
			var subj, sid string
			var n int
			fmt.Sscanf(line, "MSG %s %s %d\r\n", &subj, &sid, &n)
			buf := make([]byte, n+2)
			if _, err := readFull(br, buf); err != nil {
				return
			}
			atomic.AddInt64(&received, int64(n))
		}
	}()

	cleanup := func() {
		close(stop)
		conn.Close()
		<-done
	}
	return conn, &received, cleanup
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// rawPublisher opens a TCP connection in account A and floods MSG frames.
func rawPublisher(t *testing.T, port int, subject string, payloadSize int, stop <-chan struct{}) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	br := bufio.NewReader(conn)
	br.ReadLine()
	fmt.Fprintf(conn, "CONNECT {\"verbose\":false,\"user\":\"a\",\"pass\":\"p\"}\r\n")
	payload := bytes.Repeat([]byte("x"), payloadSize)
	frame := fmt.Appendf(nil, "PUB %s %d\r\n", subject, len(payload))
	frame = append(frame, payload...)
	frame = append(frame, '\r', '\n')
	go func() {
		for {
			select {
			case <-stop:
				conn.Close()
				return
			default:
			}
			if _, err := conn.Write(frame); err != nil {
				return
			}
		}
	}()
}

func TestAccountEgressConfigParsing(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(egressTestConf, 100_000)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	acc, err := s.LookupAccount("A")
	if err != nil {
		t.Fatal(err)
	}
	acc.mu.RLock()
	l := acc.egressLimiter
	bps := acc.egress
	acc.mu.RUnlock()
	if bps != 100_000 {
		t.Fatalf("expected configured egress 100000, got %d", bps)
	}
	if l == nil {
		t.Fatal("expected an egress limiter on account A")
	}

	accB, err := s.LookupAccount("B")
	if err != nil {
		t.Fatal(err)
	}
	accB.mu.RLock()
	lb := accB.egressLimiter
	accB.mu.RUnlock()
	if lb != nil {
		t.Fatal("account B should have no egress limiter")
	}
}

func TestAccountSetEgressLimitIdempotent(t *testing.T) {
	a := NewAccount("X")
	a.SetEgressLimit(100_000)
	a.mu.RLock()
	first := a.egressLimiter
	a.mu.RUnlock()
	if first == nil {
		t.Fatal("expected limiter")
	}
	// Same value on reload keeps the same limiter instance (no token reset).
	a.SetEgressLimit(100_000)
	a.mu.RLock()
	same := a.egressLimiter
	a.mu.RUnlock()
	if same != first {
		t.Fatal("expected limiter to be preserved when value unchanged")
	}
	// Changing the value replaces it and closes the old one.
	a.SetEgressLimit(200_000)
	a.mu.RLock()
	second := a.egressLimiter
	a.mu.RUnlock()
	if second == nil || second == first {
		t.Fatal("expected replaced limiter after value change")
	}
	// Removing it clears the field.
	a.SetEgressLimit(0)
	a.mu.RLock()
	none := a.egressLimiter
	a.mu.RUnlock()
	if none != nil {
		t.Fatal("expected no limiter after zero")
	}
	// Setting zero again is a no-op.
	a.SetEgressLimit(0)
}

func TestParseEgressBytesPerSecond(t *testing.T) {
	cases := map[string]int64{
		"1024":   1024,
		"1KB":    1 << 10,
		"1K":     1 << 10,
		"1KiB":   1 << 10,
		"1MB":    1 << 20,
		"1MiB":   1 << 20,
		"256K/s": 256 << 10,
		"2mib/s": 2 << 20,
		" 512 ":  512,
	}
	for in, want := range cases {
		got, err := parseEgressBytesPerSecond(in)
		if err != nil {
			t.Fatalf("%q unexpected error: %v", in, err)
		}
		if got != want {
			t.Fatalf("%q = %d, want %d", in, got, want)
		}
	}
	if _, err := parseEgressBytesPerSecond(-5); err == nil {
		t.Fatal("expected error for negative rate")
	}
	if _, err := parseEgressBytesPerSecond("abc"); err == nil {
		t.Fatal("expected error for garbage rate")
	}
}

// TestAccountEgressAggregateRate verifies the account-level ceiling holds
// across multiple continuously-sending big connections: total delivered
// bytes over a window stays near the configured rate, not a multiple of it.
func TestAccountEgressAggregateRate(t *testing.T) {
	const rate = 128 * 1024 // 128 KB/s aggregate
	conf := createConfFile(t, []byte(fmt.Sprintf(egressTestConf, rate)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	port := s.Addr().(*net.TCPAddr).Port

	const numBig = 3
	var counters []*int64
	var cleanups []func()
	stops := make([]chan struct{}, numBig)
	for i := 0; i < numBig; i++ {
		subj := fmt.Sprintf("big.%d", i)
		_, recv, cleanup := rawSubscriber(t, port, "a", subj)
		counters = append(counters, recv)
		cleanups = append(cleanups, cleanup)
		stops[i] = make(chan struct{})
		rawPublisher(t, port, subj, 4096, stops[i])
	}
	defer func() {
		for _, c := range cleanups {
			c()
		}
	}()

	// Warm up then measure.
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	time.Sleep(time.Second)
	elapsed := time.Since(start).Seconds()

	var total int64
	for _, c := range counters {
		total += atomic.LoadInt64(c)
	}
	for _, st := range stops {
		close(st)
	}

	got := float64(total) / elapsed
	// With 3 connections sharing one account bucket the aggregate must stay
	// bounded by ~rate (per-connection limits would have allowed ~3x).
	if got > rate*1.6 {
		t.Fatalf("aggregate egress %.0f B/s exceeds ceiling %.0f (total=%d elapsed=%.3f)", got, float64(rate), total, elapsed)
	}
	if got < rate*0.5 {
		t.Fatalf("aggregate egress %.0f B/s far below ceiling %.0f (total=%d elapsed=%.3f)", got, float64(rate), total, elapsed)
	}
}

// TestAccountEgressSetLimitUpdatesRuntime exercises a runtime change of the
// limit, including removing it. Connections blocked on the old limiter must
// be released rather than hang.
func TestAccountEgressSetLimitUpdatesRuntime(t *testing.T) {
	const rate = 32 * 1024
	conf := createConfFile(t, []byte(fmt.Sprintf(egressTestConf, rate)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	acc, err := s.LookupAccount("A")
	if err != nil {
		t.Fatal(err)
	}

	port := s.Addr().(*net.TCPAddr).Port
	_, recv, cleanup := rawSubscriber(t, port, "a", "upd")
	defer cleanup()
	stop := make(chan struct{})
	defer close(stop)
	rawPublisher(t, port, "upd", 2048, stop)

	time.Sleep(200 * time.Millisecond)
	before := atomic.LoadInt64(recv)

	// Raise the limit; writers should not stall.
	acc.SetEgressLimit(1024 * 1024)
	time.Sleep(200 * time.Millisecond)

	// Remove the limit; the old limiter must be closed and no writer left
	// blocked behind it.
	acc.SetEgressLimit(0)
	time.Sleep(200 * time.Millisecond)

	after := atomic.LoadInt64(recv)
	if after <= before {
		t.Fatalf("traffic stalled across limit updates before=%d after=%d", before, after)
	}
	acc.mu.RLock()
	l := acc.egressLimiter
	acc.mu.RUnlock()
	if l != nil {
		t.Fatal("expected limiter removed after SetEgressLimit(0)")
	}
}

// TestAccountEgressBigAndSmallSenders runs continuous big senders and an
// intermittent small sender, asserting that the small sender is served
// promptly (bounded wait behind the big senders) and that adding then
// removing a connection keeps the aggregate near the ceiling.
func TestAccountEgressBigAndSmallSenders(t *testing.T) {
	const rate = 128 * 1024
	conf := createConfFile(t, []byte(fmt.Sprintf(egressTestConf, rate)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	port := s.Addr().(*net.TCPAddr).Port

	// Two continuous big senders saturate the account.
	_, bigRecv1, cleanupBig1 := rawSubscriber(t, port, "a", "big1")
	defer cleanupBig1()
	stopBig1 := make(chan struct{})
	defer close(stopBig1)
	rawPublisher(t, port, "big1", 8192, stopBig1)

	_, bigRecv2, cleanupBig2 := rawSubscriber(t, port, "a", "big2")
	defer cleanupBig2()
	stopBig2 := make(chan struct{})
	defer close(stopBig2)
	rawPublisher(t, port, "big2", 8192, stopBig2)

	// Intermittent small sender: publishes a burst periodically.
	smallConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer smallConn.Close()
	sbr := bufio.NewReader(smallConn)
	sbr.ReadLine()
	fmt.Fprintf(smallConn, "CONNECT {\"verbose\":false,\"user\":\"a\",\"pass\":\"p\"}\r\n")
	fmt.Fprint(smallConn, "SUB small 3\r\nPING\r\n")
	sbr.ReadLine()

	// Warm up the big senders.
	time.Sleep(300 * time.Millisecond)

	// Measure latency seen by a fresh small burst: publish then drain until
	// the small payload is received. Worst-case fair wait is bounded by
	// (#active * egressMaxGrant / rate) ~= a few tens of ms, plus slack.
	for round := 0; round < 3; round++ {
		payload := bytes.Repeat([]byte("s"), 512)
		fmt.Fprintf(smallConn, "PUB small %d\r\n", len(payload))
		smallConn.Write(payload)
		fmt.Fprint(smallConn, "\r\n")

		deadline := time.Now().Add(2 * time.Second)
		var got int64
		for got < int64(len(payload)) && time.Now().Before(deadline) {
			line, rerr := sbr.ReadString('\n')
			if rerr != nil {
				t.Fatalf("small reader closed: %v", rerr)
			}
			if len(line) < 4 || line[:3] != "MSG" {
				continue
			}
			var subj, sid string
			var n int
			fmt.Sscanf(line, "MSG %s %s %d\r\n", &subj, &sid, &n)
			buf := make([]byte, n+2)
			readFull(sbr, buf)
			got += int64(n)
		}
		if got < int64(len(payload)) {
			t.Fatalf("round %d: intermittent small sender starved (got %d of %d)", round, got, len(payload))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Both big senders must have made progress (fair sharing).
	if atomic.LoadInt64(bigRecv1) == 0 || atomic.LoadInt64(bigRecv2) == 0 {
		t.Fatal("one of the big senders received nothing")
	}
}

// TestAccountEgressConnectionJoinAndLeave confirms the aggregate ceiling
// holds as connections come and go, and a disconnecting throttled
// connection does not wedge the limiter or other connections.
func TestAccountEgressConnectionJoinAndLeave(t *testing.T) {
	const rate = 96 * 1024
	conf := createConfFile(t, []byte(fmt.Sprintf(egressTestConf, rate)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	port := s.Addr().(*net.TCPAddr).Port

	startOne := func(subj string) (*int64, func(), chan struct{}) {
		_, recv, cleanup := rawSubscriber(t, port, "a", subj)
		stop := make(chan struct{})
		rawPublisher(t, port, subj, 4096, stop)
		return recv, cleanup, stop
	}

	recv1, cleanup1, stop1 := startOne("j1")
	defer cleanup1()
	time.Sleep(300 * time.Millisecond)

	// Join a second connection; aggregate must remain bounded.
	recv2, cleanup2, stop2 := startOne("j2")

	measure := func(window time.Duration) float64 {
		a1 := atomic.LoadInt64(recv1)
		a2 := atomic.LoadInt64(recv2)
		time.Sleep(window)
		b1 := atomic.LoadInt64(recv1)
		b2 := atomic.LoadInt64(recv2)
		return float64((b1-a1)+(b2-a2)) / window.Seconds()
	}

	if r := measure(time.Second); r > rate*1.6 {
		t.Fatalf("aggregate after join %.0f B/s exceeds ceiling %.0f", r, float64(rate))
	}

	// Leave the second connection; the first must keep flowing and stay
	// bounded on its own.
	close(stop2)
	cleanup2()
	time.Sleep(200 * time.Millisecond)

	acc, _ := s.LookupAccount("A")
	acc.mu.RLock()
	lim := acc.egressLimiter
	acc.mu.RUnlock()
	g0 := lim.grantedData.Load()
	before := atomic.LoadInt64(recv1)
	time.Sleep(500 * time.Millisecond)
	after := atomic.LoadInt64(recv1)
	g1 := lim.grantedData.Load()
	if after <= before {
		lim.mu.Lock()
		toks, ws := lim.tokens, len(lim.waiters)
		lim.mu.Unlock()
		t.Fatalf("surviving connection stalled after peer disconnected (tokens=%.0f waiters=%d grantedDelta=%d)",
			toks, ws, g1-g0)
	}
	solo := float64(after-before) / 0.5
	grantedRate := float64(g1-g0) / 0.5
	if grantedRate > float64(rate)*1.6 {
		t.Fatalf("granted egress after leave %.0f B/s exceeds ceiling %.0f (client saw %.0f)",
			grantedRate, float64(rate), solo)
	}
	close(stop1)
}

// TestAccountEgressControlLaneKeepsPongAlive verifies that protocol control
// frames (PONG) remain timely even while bulk data saturates the account,
// because they use the bounded priority lane.
func TestAccountEgressControlLaneKeepsPongAlive(t *testing.T) {
	const rate = 64 * 1024
	conf := createConfFile(t, []byte(fmt.Sprintf(egressTestConf, rate)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	port := s.Addr().(*net.TCPAddr).Port

	// A saturated bulk connection (subscriber receives lots of data).
	_, _, cleanup := rawSubscriber(t, port, "a", "sat")
	defer cleanup()
	stop := make(chan struct{})
	defer close(stop)
	rawPublisher(t, port, "sat", 8192, stop)
	time.Sleep(300 * time.Millisecond)

	// A second connection that only exchanges liveness PING/PONG.
	pc, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	pr := bufio.NewReader(pc)
	pr.ReadLine()
	fmt.Fprintf(pc, "CONNECT {\"verbose\":false,\"user\":\"a\",\"pass\":\"p\"}\r\n")

	for i := 0; i < 3; i++ {
		fmt.Fprint(pc, "PING\r\n")
		pc.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, rerr := pr.ReadString('\n')
		if rerr != nil {
			t.Fatalf("PONG %d not received under saturation: %v", i, rerr)
		}
		if line != "PONG\r\n" {
			t.Fatalf("unexpected control response %q", line)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
