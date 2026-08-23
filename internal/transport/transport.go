// Package transport abstracts the network so the SWIM protocol can run over
// either real UDP sockets or a deterministic in-memory simulator.
//
// The whole point of this interface is testability: the hardest thing to test
// honestly in SWIM is false-positive failure detection under packet loss and
// latency. A fake Transport (Этап 5) injects controlled drop/delay so those
// scenarios become deterministic table tests instead of flaky network luck.
// Production code depends only on Transport, never on *net.UDPConn directly.
package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// Packet is one datagram: an opaque payload and the peer address it came
// from / goes to. Encoding/decoding of the payload lives in internal/protocol.
type Packet struct {
	Addr    string // "host:port"
	Payload []byte
}

// Transport is the port every node uses to exchange datagrams. Both the real
// UDP adapter and the in-memory simulator implement it; the SWIM core is
// written against this interface only.
type Transport interface {
	// Send delivers payload to addr. It is best-effort and connectionless:
	// like UDP, delivery is not guaranteed (the simulator may drop it).
	Send(ctx context.Context, addr string, payload []byte) error
	// Receive blocks until the next inbound packet or ctx cancellation.
	Receive(ctx context.Context) (Packet, error)
	// LocalAddr is this node's own address.
	LocalAddr() string
	// Close releases the underlying socket / simulator registration.
	Close() error
}

// maxDatagram is the theoretical max UDP payload on IPv4; the read buffer is
// sized to it so no datagram is ever truncated.
const maxDatagram = 65507

// UDP is the production Transport backed by a localhost UDP socket.
type UDP struct {
	conn      *net.UDPConn
	local     string
	inbound   chan Packet
	closeOnce sync.Once
	done      chan struct{} // closed by Close to stop the receive loop
}

// compile-time assertion that UDP satisfies the port. Do not remove — it keeps
// the adapter honest as the interface evolves.
var _ Transport = (*UDP)(nil)

// NewUDP binds a UDP socket on addr (e.g. "127.0.0.1:7947") and starts its
// receive loop.
func NewUDP(addr string) (*UDP, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", addr, err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %q: %w", addr, err)
	}
	u := &UDP{
		conn: conn,
		// LocalAddr after bind reports the real port even when addr used ":0".
		local:   conn.LocalAddr().String(),
		inbound: make(chan Packet, 64),
		done:    make(chan struct{}),
	}
	go u.readLoop()
	return u, nil
}

func (u *UDP) readLoop() {
	buf := make([]byte, maxDatagram)
	for {
		n, remote, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			// After Close() the read fails by design; any other socket error
			// also ends the loop — UDP gives us no way to recover mid-stream.
			return
		}
		// Copy out of the shared read buffer so consumers never race with the
		// next ReadFromUDP overwriting it.
		payload := make([]byte, n)
		copy(payload, buf[:n])
		pkt := Packet{Addr: remote.String(), Payload: payload}
		select {
		case u.inbound <- pkt:
		case <-u.done:
			return
		}
	}
}

func (u *UDP) Send(ctx context.Context, addr string, payload []byte) error {
	// Best-effort like UDP itself: only real socket errors are reported;
	// silent loss is not observable and not an error.
	if err := ctx.Err(); err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	_, err = u.conn.WriteToUDP(payload, udpAddr)
	return err
}

func (u *UDP) Receive(ctx context.Context) (Packet, error) {
	select {
	case pkt := <-u.inbound:
		return pkt, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

func (u *UDP) LocalAddr() string { return u.local }

func (u *UDP) Close() error {
	var err error
	u.closeOnce.Do(func() {
		close(u.done)
		err = u.conn.Close()
	})
	return err
}
