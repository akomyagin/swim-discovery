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
	// Этап 2: an empty batch is no longer the invariant — A's own record is
	// queued for gossip from NewList, so the Ack must announce it. listA
	// knows nothing but itself, so the batch must be exactly {A}, not just
	// contain A among possible extras/duplicates.
	if len(ack.Updates) != 1 {
		t.Fatalf("ack Updates = %+v, want exactly 1 record (A's self)", ack.Updates)
	}
	if got := ack.Updates[0]; got.ID != "A" || got.State != member.StateAlive {
		t.Errorf("ack Updates[0] = %+v, want A's self record piggybacked as alive", got)
	}
}

// TestNode_PingCarriesGossip proves the join becomes two-sided: A only pings B,
// yet B learns about A from the piggybacked self-announcement on that Ping.
func TestNode_PingCarriesGossip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trA := network.Endpoint("A")
	defer trA.Close()
	trB := network.Endpoint("B")
	defer trB.Close()

	listA := member.NewList(member.Member{ID: "A", Addr: "A", State: member.StateAlive})
	listA.Merge(member.Member{ID: "B", Addr: "B", State: member.StateAlive})
	// B knows nothing but itself: only gossip can introduce A.
	listB := member.NewList(member.Member{ID: "B", Addr: "B", State: member.StateAlive})

	cfg := Config{ProbeInterval: time.Hour, RTTTimeout: 2 * time.Second}
	cfgA := cfg
	cfgA.Rand = rand.New(rand.NewSource(1))
	nodeA := NewNode(listA, trA, cfgA)
	nodeB := NewNode(listB, trB, cfg)

	stopA := startNode(t, nodeA)
	defer stopA()
	stopB := startNode(t, nodeB)
	defer stopB()

	// By the time probeOnce returns with the Ack, B has already merged the
	// Ping's Updates (receiveLoop applies gossip before replying).
	nodeA.probeOnce(ctx)

	var a *member.Member
	for _, m := range listB.Members() {
		if m.ID == "A" {
			mm := m
			a = &mm
			break
		}
	}
	if a == nil {
		t.Fatalf("A missing from listB after A's ping; listB = %+v", listB.Members())
	}
	if a.State != member.StateAlive {
		t.Errorf("A state in listB = %v, want alive", a.State)
	}
}

// TestNode_ReceiveAbsorbsGossip proves inbound piggybacked Updates are merged
// into the local view, and that the Ack does NOT echo them straight back to
// the peer that just supplied them (X already knows Z — re-sending it wastes
// budget that should go toward peers who don't have it yet), while A's own
// self-announcement still rides along.
func TestNode_ReceiveAbsorbsGossip(t *testing.T) {
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

	ping := protocol.Message{
		Kind:  protocol.KindPing,
		From:  "X",
		SeqNo: 1,
		Updates: []protocol.Update{
			{ID: "Z", Addr: "Z", Incarnation: 1, State: member.StateAlive},
		},
	}
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

	// The rumor about Z must be in A's view now.
	var z *member.Member
	for _, m := range listA.Members() {
		if m.ID == "Z" {
			mm := m
			z = &mm
			break
		}
	}
	if z == nil {
		t.Fatalf("Z missing from listA after gossip; listA = %+v", listA.Members())
	}
	if z.State != member.StateAlive || z.Incarnation != 1 {
		t.Errorf("Z in listA = %+v, want alive@1", *z)
	}

	// Z just changed, so Merge queued it for re-gossip — but the Ack must NOT
	// echo it back to X, who just supplied it and obviously already knows it.
	// A's own self-announcement (still unspent from NewList) should ride
	// along instead.
	foundZ, foundSelf := false, false
	for _, u := range ack.Updates {
		if u.ID == "Z" {
			foundZ = true
		}
		if u.ID == "A" {
			foundSelf = true
		}
	}
	if foundZ {
		t.Errorf("ack Updates = %+v, want Z excluded (X just supplied it)", ack.Updates)
	}
	if !foundSelf {
		t.Errorf("ack Updates = %+v, want A's self-announcement", ack.Updates)
	}
}

// TestCluster_GossipConvergence is the headline test of Этап 2: 7 nodes, each
// initially knowing only its ring neighbor, must all discover the full cluster
// purely via gossip piggybacked on probe traffic. The sparse ring topology
// forces rumors to spread epidemically — nobody starts with a full view.
func TestCluster_GossipConvergence(t *testing.T) {
	const numNodes = 7
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()

	addrs := make([]string, numNodes)
	lists := make([]*member.List, numNodes)
	nodes := make([]*Node, numNodes)
	for i := 0; i < numNodes; i++ {
		addrs[i] = "n" + string(rune('0'+i))
	}
	for i := 0; i < numNodes; i++ {
		tr := network.Endpoint(addrs[i])
		defer tr.Close()
		lists[i] = member.NewList(member.Member{
			ID:    member.ID(addrs[i]),
			Addr:  addrs[i],
			State: member.StateAlive,
		})
		// Ring join: n_i knows only n_{(i+1)%numNodes}.
		next := addrs[(i+1)%numNodes]
		lists[i].Merge(member.Member{ID: member.ID(next), Addr: next, State: member.StateAlive})
		nodes[i] = NewNode(lists[i], tr, Config{
			ProbeInterval: 10 * time.Millisecond,
			RTTTimeout:    50 * time.Millisecond,
			// Distinct fixed seeds: target choice is deterministic per node,
			// not global-rand luck.
			Rand: rand.New(rand.NewSource(int64(i + 1))),
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			n.Run(ctx)
		}(nodes[i])
	}

	// converged reports whether every node sees every ID alive; on failure it
	// returns a diagnostic of who is missing what.
	converged := func() (bool, string) {
		for i := 0; i < numNodes; i++ {
			seen := map[member.ID]member.State{}
			for _, m := range lists[i].Members() {
				seen[m.ID] = m.State
			}
			for j := 0; j < numNodes; j++ {
				st, ok := seen[member.ID(addrs[j])]
				if !ok {
					return false, addrs[i] + " has not seen " + addrs[j]
				}
				if st != member.StateAlive {
					return false, addrs[i] + " sees " + addrs[j] + " as " + st.String()
				}
			}
		}
		return true, ""
	}

	deadline := time.After(8 * time.Second)
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		ok, diag := converged()
		if ok {
			break
		}
		select {
		case <-poll.C:
		case <-deadline:
			cancel()
			wg.Wait()
			t.Fatalf("cluster did not converge before deadline: %s", diag)
		}
	}

	cancel()
	wg.Wait()
}
