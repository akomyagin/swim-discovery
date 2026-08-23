package transport

import (
	"context"
	"fmt"
	"sync"
)

// FakeNetwork is the shared in-memory switch that routes datagrams between
// registered fake endpoints. It is lossless and zero-delay by default; the
// only controlled loss is DropLink's directed per-link drop (the Этап 3
// minimum). The full drop-rate/delay/partition simulator is Этап 5.
type FakeNetwork struct {
	mu        sync.RWMutex
	endpoints map[string]*Fake // addr -> endpoint
	// dropped marks directed links (from, to) whose packets silently vanish.
	dropped map[[2]string]bool
}

// NewFakeNetwork creates an empty in-memory network.
func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{
		endpoints: map[string]*Fake{},
		dropped:   map[[2]string]bool{},
	}
}

// DropLink makes every packet sent from `from` to `to` vanish (one-way).
// Minimal knob for the Этап 3 false-positive test; the full drop/delay/
// partition simulator is Этап 5 — do not grow this into rates or delays.
func (n *FakeNetwork) DropLink(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropped[[2]string{from, to}] = true
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

// Fake is an in-memory Transport endpoint on a FakeNetwork: lossless unless a
// DropLink covers the destination.
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
	drop := f.net.dropped[[2]string{f.local, addr}]
	f.net.mu.RUnlock()
	if !ok || drop {
		// Best-effort, like UDP to a dark address (or across a dropped link):
		// the packet vanishes, the transport reports no error.
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
