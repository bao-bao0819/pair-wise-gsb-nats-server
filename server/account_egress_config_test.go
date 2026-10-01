package server

import (
	"fmt"
	"testing"
	"time"
)

func TestParseByteRate(t *testing.T) {
	cases := []struct {
		in   any
		want int64
		ok   bool
	}{
		{int64(1024), 1024, true},
		{int(4096), 4096, true},
		{"1024", 1024, true},
		{"1KB", 1024, true},
		{"1K", 1024, true},
		{"1k", 1024, true},
		{"2MB", 2 * 1024 * 1024, true},
		{"2m", 2 * 1024 * 1024, true},
		{"1G", 1024 * 1024 * 1024, true},
		{" 512 K ", 512 * 1024, true},
		{"0", 0, true},
		{"abc", 0, false},
		{"-5", 0, false},
		{"", 0, false},
		{float64(1), 0, false},
	}
	for _, tc := range cases {
		got, err := parseByteRate(tc.in, nil)
		if tc.ok {
			if err != nil {
				t.Errorf("parseByteRate(%v) unexpected error: %v", tc.in, err)
				continue
			}
			if got != tc.want {
				t.Errorf("parseByteRate(%v) = %d, want %d", tc.in, got, tc.want)
			}
		} else if err == nil {
			t.Errorf("parseByteRate(%v) expected error, got %d", tc.in, got)
		}
	}
}

func TestAccountEgressConfigParsing(t *testing.T) {
	cf := createConfFile(t, []byte(`
port: -1
accounts {
	EG {
		users = [{user: u, password: p}]
		limits {
			max_egress: "256KB"
		}
	}
	EG2 {
		users = [{user: v, password: p}]
		limits {
			max_egress_bandwidth: 131072
		}
	}
}
`))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	acc, err := s.lookupAccount("EG")
	require_NoError(t, err)
	if acc.egressBps != 256*1024 {
		t.Fatalf("expected 256KB, got %d", acc.egressBps)
	}
	if l := acc.egressLimiter(); l == nil || l.rate != 256*1024 {
		t.Fatalf("expected active limiter at 256KB, got %v", l)
	}

	acc2, err := s.lookupAccount("EG2")
	require_NoError(t, err)
	if acc2.egressBps != 131072 {
		t.Fatalf("expected 131072, got %d", acc2.egressBps)
	}
}

func TestAccountEgressReloadReleasesWaiters(t *testing.T) {
	config := func(limit string) string {
		return fmt.Sprintf(`
port: -1
accounts {
	EG {
		users = [{user: u, password: p}]
		limits {
			max_egress: %s
		}
	}
}
`, limit)
	}

	s, _, configFile := runReloadServerWithContent(t, []byte(config(`65536`)))
	defer s.Shutdown()

	acc, err := s.lookupAccount("EG")
	require_NoError(t, err)
	l := acc.egressLimiter()
	if l == nil || l.rate != 65536 {
		t.Fatalf("expected initial limiter 65536, got %v", l)
	}

	// Drain the initial burst, then block a waiter. Reloading with a much
	// higher ceiling must release it promptly (config update releases waiters).
	if !l.reserve(int(l.burst), false, nil) {
		t.Fatalf("burst drain failed")
	}
	got := make(chan struct{}, 1)
	go func() {
		for {
			if l.reserve(aelMaxChunk, false, nil) {
				close(got)
				return
			}
		}
	}()

	time.Sleep(30 * time.Millisecond)
	changeCurrentConfigContentWithNewContent(t, configFile, []byte(config(`1048576`)))
	require_NoError(t, s.Reload())

	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatalf("reload did not release blocked waiter")
	}

	if l2 := acc.egressLimiter(); l2 != l || l2.rate != 1048576 {
		t.Fatalf("expected same limiter updated to 1MB, got %+v", l2)
	}

	// Reloading with the limit removed disables gating entirely.
	changeCurrentConfigContentWithNewContent(t, configFile, []byte(config(`0`)))
	require_NoError(t, s.Reload())
	if l2 := acc.egressLimiter(); l2 != nil {
		t.Fatalf("expected limiter removed on reload, got %v", l2)
	}
}
