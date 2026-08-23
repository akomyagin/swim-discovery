package transport

import (
	"context"
	"fmt"
	"sync"
)

// FakeNetwork is the shared in-memory switch that routes datagrams between
// registered fake endpoints. Этап 1: lossless, zero-delay — it exists so swim
// ping/ack can be tested without real sockets. Этап 5 adds per-link drop-rate,
// delay and partition here.
type FakeNetwork struct {
	mu        sync.RWMutex
	endpoints map[string]*Fake // addr -> endpoint
}

// NewFakeNetwork creates an empty in-memory network.
func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{endpoints: map[string]*Fake{}}
}

// Endpoint creates and registers a Fake transport bound to addr on this
// network. Registering the same addr twice is a test-setup bug, so it panics
// rather than silently rerouting traffic.
func (n *FakeNetwork) Endpoint(addr string) *Fake {
	f := &Fake{
		net:     n,
		local:   addr,
		inbound: make(chan Packet, 64),
		done:    make(chan struct{}),
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.endpoints[addr]; exists {
		panic(fmt.Sprintf("transport: fake endpoint %q already registered", addr))
	}
	n.endpoints[addr] = f
	return f
}

// Fake is an in-memory Transport endpoint on a FakeNetwork. Этап 1: lossless.
type Fake struct {
	net       *FakeNetwork
	local     string
	inbound   chan Packet
	closeOnce sync.Once
	done      chan struct{}
}

// compile-time assertion; do not remove — it keeps the adapter honest.
var _ Transport = (*Fake)(nil)

func (f *Fake) Send(ctx context.Context, addr string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.net.mu.RLock()
	dst, ok := f.net.endpoints[addr]
	f.net.mu.RUnlock()
	if !ok {
		// Best-effort, like UDP to a dark address: the packet vanishes, the
		// transport reports no error.
		return nil
	}
	// Copy so sender and receiver never share a buffer (data-race safety).
	p := make([]byte, len(payload))
	copy(p, payload)
	pkt := Packet{Addr: f.local, Payload: p}
	// Block until delivered rather than drop on a full buffer: controlled loss
	// belongs to Этап 5, tests here must never lose messages.
	select {
	case dst.inbound <- pkt:
	case <-dst.done:
	}
	return nil
}

func (f *Fake) Receive(ctx context.Context) (Packet, error) {
	select {
	case pkt := <-f.inbound:
		return pkt, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

func (f *Fake) LocalAddr() string { return f.local }

func (f *Fake) Close() error {
	f.closeOnce.Do(func() {
		close(f.done)
		f.net.mu.Lock()
		delete(f.net.endpoints, f.local)
		f.net.mu.Unlock()
	})
	return nil
}
