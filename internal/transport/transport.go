// Package transport abstracts the network so the SWIM protocol can run over
// either real UDP sockets or a deterministic in-memory simulator.
//
// The whole point of this interface is testability: the hardest thing to test
// honestly in SWIM is false-positive failure detection under packet loss and
// latency. A fake Transport (Этап 5) injects controlled drop/delay so those
// scenarios become deterministic table tests instead of flaky network luck.
// Production code depends only on Transport, never on *net.UDPConn directly.
package transport

import "context"

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

// UDP is the production Transport backed by a localhost UDP socket.
//
// TODO(Этап 1): implement over net.ListenUDP. Send via WriteToUDP; Receive via
// a read loop over ReadFromUDP feeding an inbound channel. Keep it dependency
// free (stdlib net only).
type UDP struct {
	// TODO(Этап 1): *net.UDPConn, local addr, inbound chan Packet.
}

// compile-time assertion that UDP satisfies the port. Do not remove — it keeps
// the adapter honest as the interface evolves.
var _ Transport = (*UDP)(nil)

// NewUDP binds a UDP socket on addr (e.g. "127.0.0.1:7947") and starts its
// receive loop.
//
// TODO(Этап 1): implement.
func NewUDP(addr string) (*UDP, error) {
	_ = addr
	panic("TODO(Этап 1): NewUDP not implemented")
}

func (u *UDP) Send(ctx context.Context, addr string, payload []byte) error {
	_, _, _ = ctx, addr, payload
	panic("TODO(Этап 1): UDP.Send not implemented")
}

func (u *UDP) Receive(ctx context.Context) (Packet, error) {
	_ = ctx
	panic("TODO(Этап 1): UDP.Receive not implemented")
}

func (u *UDP) LocalAddr() string {
	panic("TODO(Этап 1): UDP.LocalAddr not implemented")
}

func (u *UDP) Close() error {
	panic("TODO(Этап 1): UDP.Close not implemented")
}
