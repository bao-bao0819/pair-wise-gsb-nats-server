package server

import (
	"bytes"
	"net"
)

// This file integrates the aggregate account egress limiter with the client
// write path. Reservation happens in bounded chunks inside flushOutbound so
// that:
//   - the aggregate account bucket bounds total emitted bytes (every chunk is
//     reserved before it reaches the socket);
//   - no single connection consumes more than one chunk per turn, giving the
//     account's active connections fair shares;
//   - small protocol control frames (PING/PONG/OK/-ERR) at the head of the
//     queue use the limiter's priority lane: timely, but still charged against
//     the same shared bucket, so they cannot become an unlimited bypass.
//
// Control frames stay in the ordinary FIFO outbound queue (nb); only their
// reservation classification is priority, so wire ordering is never changed.

// leadingControlBytes reports how many of the first max bytes of bufs are a
// single small protocol control frame at the head of the queue. Control
// frames recognized here are PING/PONG/OK/-ERR on plain client connections.
// Returns 0 if the head is ordinary data (e.g. an MSG/INFO) or the frame is
// larger than the bounded control lane.
func leadingControlBytes(bufs net.Buffers, max int64) int64 {
	if len(bufs) == 0 || max <= 0 {
		return 0
	}
	first := bufs[0]
	// nb buffers may pack several queued frames together; identify the length
	// of the leading control protocol line (terminated by CRLF).
	var size int
	switch {
	case bytes.HasPrefix(first, []byte(pongProto)):
		size = len(pongProto)
	case bytes.HasPrefix(first, []byte(pingProto)):
		size = len(pingProto)
	case bytes.HasPrefix(first, []byte(okProto)):
		size = len(okProto)
	case bytes.HasPrefix(first, []byte("-ERR")):
		for size < len(first) && size <= aelCtrlMax {
			if size+1 < len(first) && first[size] == '\r' && first[size+1] == '\n' {
				size += 2
				goto got
			}
			size++
		}
		return 0
	default:
		return 0
	}
got:
	if size > aelCtrlMax || int64(size) > max {
		return 0
	}
	return int64(size)
}

// bufsLen returns the sum of buffer lengths.
func bufsLen(bufs net.Buffers) int64 {
	var n int64
	for _, b := range bufs {
		n += int64(len(b))
	}
	return n
}

// splitBufsHead returns buffers covering at most n bytes of bufs. When a buffer
// straddles the boundary its prefix is copied so the original (pooled) backing
// array remains available for the tail.
func splitBufsHead(bufs net.Buffers, n int64) net.Buffers {
	if n <= 0 {
		return nil
	}
	var head net.Buffers
	consumed := int64(0)
	for _, b := range bufs {
		if consumed+int64(len(b)) <= n {
			head = append(head, b)
			consumed += int64(len(b))
			continue
		}
		part := n - consumed
		prefix := make([]byte, part)
		copy(prefix, b[:part])
		head = append(head, prefix)
		break
	}
	return head
}

// dropFrontBufs removes exactly n (written) bytes from the front of bufs,
// returning the remainder. Fully-consumed buffers are dropped; a partially
// consumed boundary buffer is re-sliced. The prefix copy made by
// splitBufsHead carries the written prefix, while the original pooled buffer
// is re-sliced here and later returned to the nb pool by the existing orig
// accounting in flushOutbound.
func dropFrontBufs(bufs net.Buffers, n int64) net.Buffers {
	if n <= 0 {
		return bufs
	}
	for len(bufs) > 0 && n >= int64(len(bufs[0])) {
		n -= int64(len(bufs[0]))
		bufs = bufs[1:]
	}
	if n > 0 && len(bufs) > 0 {
		bufs[0] = bufs[0][n:]
	}
	return bufs
}
