package transport

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestFake_SendReceive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()

	payload := []byte("ping")
	if err := a.Send(ctx, "B", payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	pkt, err := b.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if !bytes.Equal(pkt.Payload, payload) {
		t.Errorf("payload = %q, want %q", pkt.Payload, payload)
	}
	if pkt.Addr != "A" {
		t.Errorf("pkt.Addr = %q, want sender address %q", pkt.Addr, "A")
	}
}

func TestFake_SendUnknownAddr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()

	// Like UDP to a dark address: best-effort, no error, no panic.
	if err := a.Send(ctx, "Z", []byte("into the void")); err != nil {
		t.Errorf("Send to unknown addr = %v, want nil", err)
	}
}

func TestFake_Close(t *testing.T) {
	network := NewFakeNetwork()
	a := network.Endpoint("A")
	b := network.Endpoint("B")
	defer b.Close()

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Errorf("second Close = %v, want nil (idempotent)", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Sending to a closed endpoint must neither error nor panic.
	if err := b.Send(ctx, "A", []byte("late")); err != nil {
		t.Errorf("Send to closed endpoint = %v, want nil", err)
	}

	// Receive on the closed endpoint returns via ctx, not a stuck goroutine.
	rctx, rcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer rcancel()
	if _, err := a.Receive(rctx); err == nil {
		t.Error("Receive on closed endpoint = nil error, want ctx deadline error")
	}
}

// TestFake_DropLink proves the drop is directed: A->B vanishes silently while
// the reverse link B->A keeps delivering.
func TestFake_DropLink(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()

	network.DropLink("A", "B")

	// Dropped direction: best-effort, no error — and nothing arrives at B.
	if err := a.Send(ctx, "B", []byte("lost")); err != nil {
		t.Fatalf("Send over dropped link = %v, want nil", err)
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer rcancel()
	if pkt, err := b.Receive(rctx); err == nil {
		t.Errorf("B received %q over a dropped link, want nothing", pkt.Payload)
	}

	// Reverse direction still works.
	if err := b.Send(ctx, "A", []byte("back")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	pkt, err := a.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive on intact reverse link: %v", err)
	}
	if !bytes.Equal(pkt.Payload, []byte("back")) {
		t.Errorf("payload = %q, want %q", pkt.Payload, "back")
	}
}

// ---- Этап 5: drop-rate / partition / delay simulator ----------------------

// recvNow non-blockingly pops the next packet from f's inbound buffer. It is
// only valid right after a zero-delay Send: immediate delivery is synchronous
// (Send blocks until the packet is buffered), so by the time Send returns the
// packet either sits in the buffer or was dropped — no timeout waiting needed,
// which keeps the lossy tests fast and timing-independent.
func recvNow(f *Fake) (Packet, bool) {
	select {
	case pkt := <-f.inbound:
		return pkt, true
	default:
		return Packet{}, false
	}
}

// TestFake_SetDropRate_Deterministic: the same seed must replay the exact same
// delivery pattern — the determinism is the point; the ~50% loss share is only
// a sanity check that the rate is actually applied.
func TestFake_SetDropRate_Deterministic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const n = 1000
	run := func(seed int64) []int {
		network := NewFakeNetworkSeed(seed)
		a := network.Endpoint("A")
		defer a.Close()
		b := network.Endpoint("B")
		defer b.Close()
		network.SetDropRate("A", "B", 0.5)

		delivered := []int{}
		for i := 0; i < n; i++ {
			if err := a.Send(ctx, "B", []byte{byte(i), byte(i >> 8)}); err != nil {
				t.Fatalf("Send #%d: %v", i, err)
			}
			if pkt, ok := recvNow(b); ok {
				delivered = append(delivered, int(pkt.Payload[0])|int(pkt.Payload[1])<<8)
			}
		}
		return delivered
	}

	first := run(1)
	if len(first) < 400 || len(first) > 600 {
		t.Errorf("delivered %d of %d at drop rate 0.5, want roughly half (400..600)", len(first), n)
	}
	second := run(1)
	if len(first) != len(second) {
		t.Fatalf("same seed delivered %d vs %d packets — the drop pattern must replay exactly", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("delivery #%d = packet %d vs %d — the same seed must replay the same sequence", i, first[i], second[i])
		}
	}
}

// TestFake_SetDropRate_ZeroAndOne pins the rate edges and the directionality:
// rate 1 loses everything one-way (DropLink equivalent), rate 0 on the reverse
// link loses nothing — deterministically, no RNG luck involved.
func TestFake_SetDropRate_ZeroAndOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()

	network.SetDropRate("A", "B", 1)
	network.SetDropRate("B", "A", 0)

	for i := 0; i < 20; i++ {
		if err := a.Send(ctx, "B", []byte("lost")); err != nil {
			t.Fatalf("Send over rate-1 link: %v", err)
		}
		if pkt, ok := recvNow(b); ok {
			t.Fatalf("B received %q over a rate-1 link, want nothing", pkt.Payload)
		}
		if err := b.Send(ctx, "A", []byte("kept")); err != nil {
			t.Fatalf("Send over rate-0 link: %v", err)
		}
		if _, ok := recvNow(a); !ok {
			t.Fatalf("rate-0 link lost packet #%d, want lossless delivery", i)
		}
	}
}

// TestFake_Partition_Isolates: nodes sharing a partition talk, links crossing
// the boundary are cut both ways, and Heal restores everything.
func TestFake_Partition_Isolates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()
	c := network.Endpoint("C")
	defer c.Close()

	delivered := func(from, to *Fake) bool {
		t.Helper()
		if err := from.Send(ctx, to.local, []byte("x")); err != nil {
			t.Fatalf("Send %s->%s: %v", from.local, to.local, err)
		}
		_, ok := recvNow(to)
		return ok
	}

	network.Partition(1, "A", "B") // C stays in the default group 0

	if !delivered(a, b) || !delivered(b, a) {
		t.Error("A and B share partition 1, packets between them must flow")
	}
	if delivered(a, c) || delivered(c, a) || delivered(b, c) || delivered(c, b) {
		t.Error("links across the partition boundary must be cut in both directions")
	}

	network.Heal()
	if !delivered(a, b) || !delivered(a, c) || !delivered(c, a) || !delivered(b, c) || !delivered(c, b) {
		t.Error("after Heal every link must deliver again")
	}
}

// TestFake_Delay_ArrivesLate: a delayed link neither blocks the sender nor
// delivers early — the packet stays "in flight" for the configured time.
func TestFake_Delay_ArrivesLate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const d = 50 * time.Millisecond
	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()
	network.SetDelay("A", "B", d)

	start := time.Now()
	if err := a.Send(ctx, "B", []byte("slow")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if since := time.Since(start); since >= d {
		t.Errorf("Send blocked for %v, want an immediate return — delay must ride a timer, not the sender", since)
	}

	// Still in flight: a receive much shorter than the delay comes up empty.
	early, ecancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer ecancel()
	if pkt, err := b.Receive(early); err == nil {
		t.Errorf("B received %q before the delay elapsed", pkt.Payload)
	}

	pkt, err := b.Receive(ctx) // generous window, well past the delay
	if err != nil {
		t.Fatalf("Receive after delay: %v", err)
	}
	if since := time.Since(start); since < d {
		t.Errorf("packet arrived after %v, want >= the %v link delay", since, d)
	}
	if !bytes.Equal(pkt.Payload, []byte("slow")) {
		t.Errorf("payload = %q, want %q", pkt.Payload, "slow")
	}
}

// TestFake_DefaultDelay_AppliesWithoutExplicitSetDelay: a link with no
// per-link SetDelay inherits the network default.
func TestFake_DefaultDelay_AppliesWithoutExplicitSetDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const d = 40 * time.Millisecond
	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()
	network.SetDefaultDelay(d)

	start := time.Now()
	if err := a.Send(ctx, "B", []byte("default-slow")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	early, ecancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer ecancel()
	if pkt, err := b.Receive(early); err == nil {
		t.Errorf("B received %q before the default delay elapsed", pkt.Payload)
	}

	if _, err := b.Receive(ctx); err != nil {
		t.Fatalf("Receive after default delay: %v", err)
	}
	if since := time.Since(start); since < d {
		t.Errorf("packet arrived after %v, want >= the %v default delay", since, d)
	}
}

// TestFake_Delay_Reordering: a delayed packet may be overtaken by a later
// immediate one — the simulator models reordering (and an explicit zero
// SetDelay restores immediate delivery on the link).
func TestFake_Delay_Reordering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()

	network.SetDelay("A", "B", 50*time.Millisecond)
	if err := a.Send(ctx, "B", []byte("delayed")); err != nil {
		t.Fatalf("Send delayed: %v", err)
	}
	network.SetDelay("A", "B", 0) // explicit zero: back to immediate
	if err := a.Send(ctx, "B", []byte("instant")); err != nil {
		t.Fatalf("Send instant: %v", err)
	}

	first, err := b.Receive(ctx)
	if err != nil {
		t.Fatalf("first Receive: %v", err)
	}
	if !bytes.Equal(first.Payload, []byte("instant")) {
		t.Errorf("first arrival = %q, want %q (the immediate packet overtakes the delayed one)", first.Payload, "instant")
	}
	second, err := b.Receive(ctx)
	if err != nil {
		t.Fatalf("second Receive: %v", err)
	}
	if !bytes.Equal(second.Payload, []byte("delayed")) {
		t.Errorf("second arrival = %q, want %q", second.Payload, "delayed")
	}
}

func TestFake_PayloadCopied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()

	buf := []byte("original")
	if err := a.Send(ctx, "B", buf); err != nil {
		t.Fatalf("Send: %v", err)
	}
	copy(buf, "mutated!")

	pkt, err := b.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if !bytes.Equal(pkt.Payload, []byte("original")) {
		t.Errorf("payload = %q, want %q (sender buffer must be copied)", pkt.Payload, "original")
	}
}
