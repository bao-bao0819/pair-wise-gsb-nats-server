package server

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// egressSubscriber is a raw NATS client subscribed to a subject, counting the
// payload bytes actually delivered (and thus actually written) by the server.
type egressSubscriber struct {
	conn net.Conn
	name string

	mu    sync.Mutex
	bytes int64
	msgs  int64

	stop chan struct{}
	done chan struct{}
}

func dialEgressSubscriber(t *testing.T, url, user, pass, name, subject string) *egressSubscriber {
	t.Helper()
	addr := strings.TrimPrefix(url, "nats://")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require_NoError(t, err)

	if user != "" {
		fmt.Fprintf(conn, "CONNECT {\"user\":%q,\"pass\":%q,\"verbose\":false}\r\n", user, pass)
	} else {
		fmt.Fprint(conn, "CONNECT {\"verbose\":false}\r\n")
	}
	fmt.Fprintf(conn, "PING\r\nSUB %s %s\r\nPING\r\n", subject, name)

	s := &egressSubscriber{
		conn: conn,
		name: name,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go s.readLoop()
	return s
}

func (s *egressSubscriber) readLoop() {
	defer close(s.done)
	buf := make([]byte, 64*1024)
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		s.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := s.conn.Read(buf)
		if err != nil {
			return
		}
		// Approximate delivered payload bytes with raw protocol bytes; the
		// messages dominate and headers are negligible, which is sufficient to
		// observe aggregate and per-connection rates.
		s.mu.Lock()
		s.bytes += int64(n)
		s.mu.Unlock()
	}
}

func (s *egressSubscriber) snapshot() (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes, s.msgs
}

func (s *egressSubscriber) close() {
	close(s.stop)
	s.conn.Close()
	<-s.done
}

// publishEgress publishes messages from a separate raw client as fast as the
// server accepts them. Each message is delivered to every subscriber, so the
// server's egress to the account is the driver being limited.
func publishEgress(t *testing.T, url, user, pass, subject string, payload []byte, stop <-chan struct{}) {
	t.Helper()
	addr := strings.TrimPrefix(url, "nats://")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require_NoError(t, err)
	defer conn.Close()
	if user != "" {
		fmt.Fprintf(conn, "CONNECT {\"user\":%q,\"pass\":%q,\"verbose\":false}\r\n", user, pass)
	} else {
		fmt.Fprint(conn, "CONNECT {\"verbose\":false}\r\n")
	}
	// Drain server INFO/PONG replies asynchronously so the publisher socket
	// never blocks on its own tiny receive buffer.
	go func() {
		buf := make([]byte, 4096)
		for {
			conn.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()

	hdr := fmt.Sprintf("PUB %s %d\r\n", subject, len(payload))
	for {
		select {
		case <-stop:
			return
		default:
		}
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write([]byte(hdr)); err != nil {
			return
		}
		if _, err := conn.Write(payload); err != nil {
			return
		}
		if _, err := conn.Write([]byte("\r\n")); err != nil {
			return
		}
	}
}

func TestAccountEgressIntegrationFairShare(t *testing.T) {
	// Keep the runtime modest for CI but long enough to measure steady state.
	const rate = 256 * 1024 // bytes/sec aggregate ceiling

	cf := createConfFile(t, []byte(fmt.Sprintf(`
port: -1
accounts {
	EG {
		users = [{user: u, password: p}]
		limits {
			max_egress: %d
		}}
	}
}
`, rate)))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	acc, err := s.lookupAccount("EG")
	require_NoError(t, err)
	if st := acc.egressStat(); st.Rate != rate {
		t.Fatalf("expected rate %d, got %d", rate, st.Rate)
	}

	url := s.ClientURL()
	payload := make([]byte, 2048)

	// Two continuously receiving "large" connections.
	subA := dialEgressSubscriber(t, url, "u", "p", "A", "heavy")
	subB := dialEgressSubscriber(t, url, "u", "p", "B", "heavy")

	pubStop := make(chan struct{})
	go publishEgress(t, url, "u", "p", "heavy", payload, pubStop)
	defer close(pubStop)

	// Let the limiter reach steady state.
	time.Sleep(500 * time.Millisecond)

	measure := func(dur time.Duration) (a, b, total int64) {
		a0, _ := subA.snapshot()
		b0, _ := subB.snapshot()
		time.Sleep(dur)
		a1, _ := subA.snapshot()
		b1, _ := subB.snapshot()
		return a1 - a0, b1 - b0, (a1 - a0) + (b1 - b0)
	}

	// Measure aggregate rate over a window. It must stay at/under the ceiling
	// (plus slack) rather than N x ceiling.
	a, b, total := measure(time.Second)
	gotRate := total
	if float64(gotRate) > float64(rate)*1.6 {
		t.Fatalf("aggregate egress %d exceeded ceiling %d by too much", gotRate, rate)
	}
	if float64(gotRate) < float64(rate)*0.5 {
		t.Fatalf("aggregate egress %d far below ceiling %d", gotRate, rate)
	}

	// Fair share: both large connections receive comparable amounts.
	if a == 0 || b == 0 {
		t.Fatalf("expected both large connections to receive data: %d vs %d", a, b)
	}
	ratio := float64(a) / float64(b)
	if ratio < 0.5 || ratio > 2.0 {
		t.Fatalf("unfair shares between large connections: %d vs %d (%.2f)", a, b, ratio)
	}

	// The account limiter observed blocking waits (proves the ceiling binds).
	if st := acc.egressStat(); st.WaitedMs == 0 {
		t.Fatalf("expected non-zero aggregate wait time")
	}

	subA.close()
	subB.close()
}

func TestAccountEgressIntegrationControlAndChurn(t *testing.T) {
	const rate = 128 * 1024

	opts := DefaultTestOptions
	opts.Port = -1
	s := RunServer(&opts)
	defer s.Shutdown()

	acc, err := s.lookupAccount("$G")
	require_NoError(t, err)
	acc.configureEgress(rate)
	defer acc.configureEgress(0)

	url := s.ClientURL()
	payload := make([]byte, 1024)

	// One continuous large connection.
	big := dialEgressSubscriber(t, url, "", "", "big", "x")

	pubStop := make(chan struct{})
	go publishEgress(t, url, "", "", "x", payload, pubStop)
	defer close(pubStop)

	time.Sleep(300 * time.Millisecond)

	// Join a new connection mid-stream: it must begin receiving (dynamic
	// membership) and take a share of the aggregate.
	late := dialEgressSubscriber(t, url, "", "", "late", "x")
	time.Sleep(400 * time.Millisecond)
	got, _ := late.snapshot()
	if got == 0 {
		t.Fatalf("newly joined connection received nothing")
	}

	// An intermittent small connection that connects, pulls a little, and
	// disconnects must not be stuck waiting forever behind the large sender.
	small := dialEgressSubscriber(t, url, "", "", "small", "x")
	start := time.Now()
	deadline := time.Now().Add(2 * time.Second)
	var sawData bool
	for time.Now().Before(deadline) {
		if n, _ := small.snapshot(); n > 0 {
			sawData = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawData {
		t.Fatalf("intermittent connection got no data within %v", time.Since(start))
	}
	small.close() // disconnect must release its waiter (nothing queued here).

	// Config update that removes the limit must release any blocked waiters;
	// aggregate throughput should then be able to exceed the old ceiling.
	b0, _ := big.snapshot()
	l0, _ := late.snapshot()
	acc.configureEgress(0)
	time.Sleep(500 * time.Millisecond)
	b1, _ := big.snapshot()
	l1, _ := late.snapshot()
	window := (b1 - b0) + (l1 - l0)
	// With the limit removed over 0.5s, local loopback should move well beyond
	// what a 128KB/s cap allowed in that window.
	if window < int64(float64(rate)*0.5*2) {
		t.Fatalf("removing limit did not release throughput: %d bytes", window)
	}

	big.close()
	late.close()
}
