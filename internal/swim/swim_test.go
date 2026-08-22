// White-box tests (package swim): they drive the private probeOnce directly so
// the probe is deterministic instead of waiting on a real ticker.
package swim

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/swim-discovery/internal/member"
	"github.com/akomyagin/swim-discovery/internal/protocol"
	"github.com/akomyagin/swim-discovery/internal/transport"
)

// startNode runs node.Run in a goroutine and returns a stop func that cancels
// it and waits for completion.
func startNode(t *testing.T, node *Node) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.Run(ctx)
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

func TestNode_PingAck_ConfirmsAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trA := network.Endpoint("A")
	defer trA.Close()
	trB := network.Endpoint("B")
	defer trB.Close()

	listA := member.NewList(member.Member{ID: "A", Addr: "A", State: member.StateAlive})
	listA.Merge(member.Member{ID: "B", Addr: "B", State: member.StateAlive})
	listB := member.NewList(member.Member{ID: "B", Addr: "B", State: member.StateAlive})

	// Huge ProbeInterval keeps the ticker out of the way: the test triggers
	// the probe explicitly. RTTTimeout is deliberately generous (2s) so the
	// elapsed-time assertion below can distinguish "Ack arrived" (returns in
	// microseconds over the lossless fake transport) from "timed out"
	// (returns only after ~RTTTimeout) — B starts and stays Alive either way,
	// so the resulting state alone cannot tell those two outcomes apart.
	cfg := Config{ProbeInterval: time.Hour, RTTTimeout: 2 * time.Second}
	cfgA := cfg
	cfgA.Rand = rand.New(rand.NewSource(1))
	nodeA := NewNode(listA, trA, cfgA)
	nodeB := NewNode(listB, trB, cfg)

	stopA := startNode(t, nodeA)
	defer stopA()
	stopB := startNode(t, nodeB)
	defer stopB()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		nodeA.probeOnce(ctx)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("probeOnce did not return before test timeout")
	}
	elapsed := time.Since(start)

	// Proves the Ack was actually received rather than merely that B started
	// (and stayed) Alive regardless of outcome: a real Ack over the fake
	// transport returns almost instantly, while a missed Ack only returns
	// after the full RTTTimeout (see TestNode_ProbeTimeout_NoAck_StaysAlive).
	if elapsed >= cfg.RTTTimeout/2 {
		t.Fatalf("probeOnce took %v (RTTTimeout %v) — looks like it timed out waiting for an Ack instead of receiving one", elapsed, cfg.RTTTimeout)
	}

	var b *member.Member
	for _, m := range listA.Members() {
		if m.ID == "B" {
			mm := m
			b = &mm
			break
		}
	}
	if b == nil {
		t.Fatal("B missing from listA after probe")
	}
	if b.State != member.StateAlive {
		t.Errorf("B state = %v, want alive after Ack", b.State)
	}
}

func TestNode_ProbeTimeout_NoAck_StaysAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trA := network.Endpoint("A")
	defer trA.Close()
	// B is never started: the Ping goes nowhere and no Ack ever comes.

	listA := member.NewList(member.Member{ID: "A", Addr: "A", State: member.StateAlive})
	listA.Merge(member.Member{ID: "B", Addr: "B", State: member.StateAlive})

	nodeA := NewNode(listA, trA, Config{
		ProbeInterval: time.Hour,
		RTTTimeout:    50 * time.Millisecond,
		Rand:          rand.New(rand.NewSource(1)),
	})
	stopA := startNode(t, nodeA)
	defer stopA()

	done := make(chan struct{})
	go func() {
		defer close(done)
		nodeA.probeOnce(ctx)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("probeOnce hung past RTTTimeout")
	}

	// Этап 1 boundary: a missing Ack must NOT demote the peer — Suspect
	// transitions arrive only with Этапы 3/4.
	for _, m := range listA.Members() {
		if m.ID == "B" && m.State != member.StateAlive {
			t.Errorf("B state = %v, want alive (no Suspect in Этап 1)", m.State)
		}
	}
}

func TestNode_RespondsAckToPing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trA := network.Endpoint("A")
	defer trA.Close()
	trX := network.Endpoint("X")
	defer trX.Close()

	listA := member.NewList(member.Member{ID: "A", Addr: "A", State: member.StateAlive})
	nodeA := NewNode(listA, trA, Config{ProbeInterval: time.Hour, RTTTimeout: time.Second})
	stopA := startNode(t, nodeA)
	defer stopA()

	ping := protocol.Message{Kind: protocol.KindPing, From: "X", SeqNo: 99}
	payload, err := protocol.Encode(ping)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := trX.Send(ctx, "A", payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	pkt, err := trX.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	ack, err := protocol.Decode(pkt.Payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if ack.Kind != protocol.KindAck {
		t.Errorf("reply kind = %v, want KindAck", ack.Kind)
	}
	if ack.SeqNo != ping.SeqNo {
		t.Errorf("ack SeqNo = %d, want echoed %d", ack.SeqNo, ping.SeqNo)
	}
	if ack.From != "A" {
		t.Errorf("ack From = %q, want %q", ack.From, "A")
	}
	if len(ack.Updates) != 0 {
		t.Errorf("ack Updates = %+v, want empty in Этап 1", ack.Updates)
	}
}
