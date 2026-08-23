package transport

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// FakeNetwork is the shared in-memory switch that routes datagrams between
// registered fake endpoints. Этап 5 grew it from a lossless router into a
// deterministic network simulator: per-link drop rates, per-link (and
// network-default) delivery delays, and symmetric partitions, with the lossy
// decisions driven by a seedable RNG so a single-sender scenario replays
// exactly (concurrent senders racing for the same link may draw from the RNG
// in a different order between runs — determinism is per-sender-stream, not
// global). With no knobs touched it stays lossless and zero-delay, so
// pre-Этап-5 tests run unchanged.
type FakeNetwork struct {
	mu        sync.RWMutex
	endpoints map[string]*Fake // addr -> endpoint
	// dropRate holds the loss probability in [0,1] per directed link (from, to).
	dropRate map[[2]string]float64
	// delay holds an explicit per-link one-way delivery delay. A PRESENT zero
	// entry is meaningful: it overrides defaultDelay back to immediate delivery.
	delay        map[[2]string]time.Duration
	defaultDelay time.Duration
	// partitionOf maps addr -> partition id; an absent key means the default
	// group 0. Packets never cross partition boundaries (see Partition).
	partitionOf map[string]int

	// rng backs the drop-rate rolls. It has its own tiny mutex instead of
	// riding n.mu because *rand.Rand is not concurrency-safe and the roll
	// happens on the hot Send path, possibly from many sender goroutines at
	// once — the critical section is a single Float64, nothing more.
	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewFakeNetwork creates an empty in-memory network seeded with a fixed
// default. Determinism-by-default is a project rule; with no drop rates
// configured the RNG is never consulted, so existing lossless tests behave
// exactly as before.
func NewFakeNetwork() *FakeNetwork {
	return NewFakeNetworkSeed(1)
}

// NewFakeNetworkSeed creates an empty in-memory network whose drop-rate rolls
// are driven by the given seed, so lossy scenarios replay bit-for-bit.
func NewFakeNetworkSeed(seed int64) *FakeNetwork {
	return &FakeNetwork{
		endpoints:   map[string]*Fake{},
		dropRate:    map[[2]string]float64{},
		delay:       map[[2]string]time.Duration{},
		partitionOf: map[string]int{},
		rng:         rand.New(rand.NewSource(seed)),
	}
}

// SetDropRate makes each packet sent from `from` to `to` vanish with
// probability p (0 = never, 1 = always), one-way. Deterministic given the
// network's seed. p is clamped to [0,1].
func (n *FakeNetwork) SetDropRate(from, to string, p float64) {
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate[[2]string{from, to}] = p
}

// DropLink makes every packet sent from `from` to `to` vanish (one-way). Since
// Этап 5 it is just the 100% special case of SetDropRate, kept by name because
// the Этап 3 false-positive tests call it.
func (n *FakeNetwork) DropLink(from, to string) {
	n.SetDropRate(from, to, 1)
}

// Partition puts the given addrs into an isolated group identified by id:
// packets between a member of this group and any node NOT in it vanish (both
// directions). Calling Partition again reassigns membership. id 0 is the
// default group every unlisted node belongs to.
func (n *FakeNetwork) Partition(id int, addrs ...string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, a := range addrs {
		// A stored 0 and an absent key read identically (map zero-value), so
		// group 0 needs no special-casing here.
		n.partitionOf[a] = id
	}
}

// Heal removes all partitioning: every node returns to the default group and
// all links carry again (drop-rates set via SetDropRate stay in effect).
func (n *FakeNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitionOf = map[string]int{}
}

// SetDelay makes packets from `from` to `to` arrive after d (one-way).
// Overrides the network default for that link. Zero d restores immediate
// delivery on that link.
func (n *FakeNetwork) SetDelay(from, to string, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay[[2]string{from, to}] = d
}

// SetDefaultDelay sets the delay applied to every link that has no explicit
// SetDelay. Zero (the constructor default) means immediate delivery.
func (n *FakeNetwork) SetDefaultDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.defaultDelay = d
}

// roll draws one uniform [0,1) sample for a drop decision.
func (n *FakeNetwork) roll() float64 {
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	return n.rng.Float64()
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

// Fake is an in-memory Transport endpoint on a FakeNetwork. Delivery is
// governed by the network's knobs — drop rates, delays, partitions — and is
// lossless and immediate by default.
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
	link := [2]string{f.local, addr}
	f.net.mu.RLock()
	dst, ok := f.net.endpoints[addr]
	// Partition outranks drop-rate: a cut across the boundary is unconditional
	// and consumes no randomness, so partitioning a network never shifts the
	// seed-determined drop sequence on the surviving links.
	partitioned := f.net.partitionOf[f.local] != f.net.partitionOf[addr]
	rate := f.net.dropRate[link]
	d, explicit := f.net.delay[link]
	if !explicit {
		d = f.net.defaultDelay
	}
	f.net.mu.RUnlock()
	if !ok || partitioned {
		// Best-effort, like UDP to a dark address (or across a partition):
		// the packet vanishes, the transport reports no error.
		return nil
	}
	// rate >= 1 short-circuits without consuming the RNG, so a 100% drop
	// (DropLink) stays deterministic no matter how often the RNG was rolled
	// by other lossy links before this call.
	if rate > 0 && (rate >= 1 || f.net.roll() < rate) {
		return nil // dropped: loss is a normal SWIM scenario, not an error
	}
	// Copy so sender and receiver never share a buffer (data-race safety).
	// Mandatory BEFORE scheduling the delayed delivery below: the timer
	// goroutine must never observe the sender mutating its buffer after Send
	// has already returned.
	p := make([]byte, len(payload))
	copy(p, payload)
	pkt := Packet{Addr: f.local, Payload: p}
	if d > 0 {
		// Delayed delivery runs on REAL time, deliberately not on the swim
		// Clock — the simulator does not share a timescale with suspicion
		// timers (Этап 5 decision). The sender returns immediately, like real
		// UDP, so its wall-clock RTT window starts ticking right away; the
		// timer goroutine delivers later. Delayed packets may overtake
		// immediate ones — SWIM tolerates reordering. Deliberately no timer
		// registry for Close: at test scale the timers just fire and get
		// GC'd, and the done-select keeps a late delivery from touching a
		// closed endpoint.
		time.AfterFunc(d, func() {
			select {
			case dst.inbound <- pkt:
			case <-dst.done:
			}
		})
		return nil
	}
	// Immediate path: block until delivered rather than drop on a full buffer.
	// Only the knobs above may lose packets — never an unlucky buffer size.
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
