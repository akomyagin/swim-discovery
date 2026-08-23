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
	// after the full RTTTimeout (see TestNode_ProbeTimeout_NoIndirect_MarksSuspect).
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

// TestNode_ProbeTimeout_NoIndirect_MarksSuspect replaces Этап 1's
// TestNode_ProbeTimeout_NoAck_StaysAlive with the OPPOSITE expectation: since
// Этап 3 a fully failed probe demotes the peer. In a 2-node cluster the only
// Other IS the target, so pickMediators returns nobody, the indirect phase is
// skipped, and the direct timeout alone leads straight to Suspect — correct
// SWIM behavior when indirect probing is physically impossible.
func TestNode_ProbeTimeout_NoIndirect_MarksSuspect(t *testing.T) {
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

	found := false
	for _, m := range listA.Members() {
		if m.ID == "B" {
			found = true
			if m.State != member.StateSuspect {
				t.Errorf("B state = %v, want suspect (direct probe failed, no mediators available)", m.State)
			}
		}
	}
	if !found {
		t.Fatal("B missing from listA after probe")
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

// fullList builds a membership list for self that already knows every node in
// ids — the fixed {I, M, T} topology shared by the indirect-probe tests.
func fullList(self string, ids ...string) *member.List {
	l := member.NewList(member.Member{ID: member.ID(self), Addr: self, State: member.StateAlive})
	for _, id := range ids {
		if id != self {
			l.Merge(member.Member{ID: member.ID(id), Addr: id, State: member.StateAlive})
		}
	}
	return l
}

// TestNode_IndirectProbe_SuppressesFalsePositive is the project's central
// invariant (Этап 3): losing only the INITIATOR's packets to a live target
// must not get the target marked Suspect — a mediator's indirect ping has to
// rescue it. Topology: I, T, M all know each other; the I->T link is dropped
// (one-way!), every other link works. I's direct Ping dies, its PingReq
// reaches M, M pings T, T answers M, M relays the Ack under I's SeqNo — the
// probe closes as success and T stays Alive.
func TestNode_IndirectProbe_SuppressesFalsePositive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trI := network.Endpoint("I")
	defer trI.Close()
	trM := network.Endpoint("M")
	defer trM.Close()
	trT := network.Endpoint("T")
	defer trT.Close()

	// Only I->T vanishes; T->I, I<->M and M<->T keep working.
	network.DropLink("I", "T")

	listI := fullList("I", "I", "M", "T")
	listM := fullList("M", "I", "M", "T")
	listT := fullList("T", "I", "M", "T")

	// Generous IndirectTimeout: the relay crosses two extra hops (I->M->T->M->I)
	// but on the in-memory fake that is microseconds — the window only needs to
	// never expire first. RTTTimeout stays short because phase 1 ALWAYS burns
	// it fully here (the direct Ping is dropped), and this test runs -count=200.
	cfg := Config{ProbeInterval: time.Hour, RTTTimeout: 100 * time.Millisecond, IndirectTimeout: 2 * time.Second}
	cfgI := cfg
	// Seed 1: the first Intn(2) yields 1, so I's probe target is Others()[1]
	// == T (Others sorts by ID: [M, T]). The elapsed-time assert below still
	// verifies the choice at runtime.
	cfgI.Rand = rand.New(rand.NewSource(1))
	nodeI := NewNode(listI, trI, cfgI)
	nodeM := NewNode(listM, trM, cfg)
	nodeT := NewNode(listT, trT, cfg)

	stopI := startNode(t, nodeI)
	defer stopI()
	stopM := startNode(t, nodeM)
	defer stopM()
	stopT := startNode(t, nodeT)
	defer stopT()

	start := time.Now()
	nodeI.probeOnce(ctx)
	elapsed := time.Since(start)

	// The direct phase must have burned (most of) its RTTTimeout — the I->T
	// link is down. A near-instant return would mean the probe actually hit M
	// (wrong target) and the assertions below would pass vacuously.
	if elapsed < cfg.RTTTimeout/2 {
		t.Fatalf("probeOnce returned in %v (RTTTimeout %v) — direct ping got answered, so the probe target was not T", elapsed, cfg.RTTTimeout)
	}

	var tRec *member.Member
	for _, m := range listI.Members() {
		if m.ID == "T" {
			mm := m
			tRec = &mm
			break
		}
	}
	if tRec == nil {
		t.Fatal("T missing from listI after probe")
	}
	if tRec.State != member.StateAlive {
		t.Fatalf("T state = %v, want alive — indirect probe must suppress the false positive", tRec.State)
	}
}

// TestNode_IndirectProbe_AllFail_MarksSuspect is the negative counterpart: T
// is genuinely unreachable (never even registered on the network), so both the
// direct Ping and M's indirect ping stay silent, and the chain must terminate
// in Suspect — not hang, not leave T Alive.
func TestNode_IndirectProbe_AllFail_MarksSuspect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trI := network.Endpoint("I")
	defer trI.Close()
	trM := network.Endpoint("M")
	defer trM.Close()
	// T is never registered: every Ping to it — direct or via M — goes dark.

	listI := fullList("I", "I", "M", "T")
	listM := fullList("M", "I", "M", "T")

	// Short timeouts are safe here: every wait is a genuine timeout, nothing
	// has to "beat" a deadline, so the outcome is timing-independent.
	cfg := Config{ProbeInterval: time.Hour, RTTTimeout: 50 * time.Millisecond, IndirectTimeout: 150 * time.Millisecond}
	cfgI := cfg
	// Seed 1 again: first Intn(2) == 1 selects T from Others() == [M, T].
	cfgI.Rand = rand.New(rand.NewSource(1))
	nodeI := NewNode(listI, trI, cfgI)
	nodeM := NewNode(listM, trM, cfg)

	stopI := startNode(t, nodeI)
	defer stopI()
	stopM := startNode(t, nodeM)
	defer stopM()

	done := make(chan struct{})
	go func() {
		defer close(done)
		nodeI.probeOnce(ctx)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("probeOnce hung past both probe phases")
	}

	var tRec *member.Member
	for _, m := range listI.Members() {
		if m.ID == "T" {
			mm := m
			tRec = &mm
			break
		}
	}
	if tRec == nil {
		t.Fatal("T missing from listI after probe")
	}
	if tRec.State != member.StateSuspect {
		t.Errorf("T state = %v, want suspect after direct + indirect both failed", tRec.State)
	}
}

// TestNode_RelaysPingReq isolates the mediator role (the PingReq analogue of
// TestNode_RespondsAckToPing): a running node M gets a hand-crafted PingReq
// from a bare initiator endpoint, pings the target itself under its OWN seq,
// and after the target's Ack relays an Ack to the initiator carrying the
// initiator's ORIGINAL SeqNo.
func TestNode_RelaysPingReq(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trI := network.Endpoint("I")
	defer trI.Close()
	trM := network.Endpoint("M")
	defer trM.Close()
	trT := network.Endpoint("T")
	defer trT.Close()

	// Only M is a real node; it must know T to be able to mediate. I and T
	// are driven by hand. M not knowing I is deliberate: the relay goes to the
	// packet's source address, membership knowledge of the initiator is not
	// required.
	listM := fullList("M", "M", "T")
	nodeM := NewNode(listM, trM, Config{ProbeInterval: time.Hour, RTTTimeout: 2 * time.Second})
	stopM := startNode(t, nodeM)
	defer stopM()

	const initiatorSeq = 77
	pingReq := protocol.Message{Kind: protocol.KindPingReq, From: "I", Target: "T", SeqNo: initiatorSeq}
	payload, err := protocol.Encode(pingReq)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := trI.Send(ctx, "M", payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// T receives M's nested Ping: it must be M's own probe, under M's own seq.
	pkt, err := trT.Receive(ctx)
	if err != nil {
		t.Fatalf("T Receive: %v", err)
	}
	ping, err := protocol.Decode(pkt.Payload)
	if err != nil {
		t.Fatalf("Decode ping: %v", err)
	}
	if ping.Kind != protocol.KindPing {
		t.Fatalf("T got kind = %v, want KindPing", ping.Kind)
	}
	if ping.From != "M" {
		t.Errorf("nested ping From = %q, want %q", ping.From, "M")
	}
	if ping.SeqNo == initiatorSeq {
		t.Errorf("nested ping SeqNo = %d — mediator must use its OWN seq, not the initiator's", ping.SeqNo)
	}

	// T answers M's ping by hand.
	ackPayload, err := protocol.Encode(protocol.Message{Kind: protocol.KindAck, From: "T", SeqNo: ping.SeqNo})
	if err != nil {
		t.Fatalf("Encode ack: %v", err)
	}
	if err := trT.Send(ctx, pkt.Addr, ackPayload); err != nil {
		t.Fatalf("Send ack: %v", err)
	}

	// I receives the relay: an Ack from M carrying the initiator's SeqNo.
	relayPkt, err := trI.Receive(ctx)
	if err != nil {
		t.Fatalf("I Receive: %v", err)
	}
	relay, err := protocol.Decode(relayPkt.Payload)
	if err != nil {
		t.Fatalf("Decode relay: %v", err)
	}
	if relay.Kind != protocol.KindAck {
		t.Errorf("relay kind = %v, want KindAck", relay.Kind)
	}
	if relay.SeqNo != initiatorSeq {
		t.Errorf("relay SeqNo = %d, want the initiator's %d echoed unchanged", relay.SeqNo, initiatorSeq)
	}
	if relay.From != "M" {
		t.Errorf("relay From = %q, want %q", relay.From, "M")
	}
}

// TestNode_PingReq_TargetSilent_NoRelay fixes the mediator's failure mode:
// when its nested Ping to the target gets no Ack, the mediator sends NOTHING
// back — the protocol has no negative reply, the initiator learns of the
// failure only through its own indirect timeout.
func TestNode_PingReq_TargetSilent_NoRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	network := transport.NewFakeNetwork()
	trI := network.Endpoint("I")
	defer trI.Close()
	trM := network.Endpoint("M")
	defer trM.Close()
	// T is never registered: M's nested Ping goes dark.

	listM := fullList("M", "M", "T")
	nodeM := NewNode(listM, trM, Config{ProbeInterval: time.Hour, RTTTimeout: 50 * time.Millisecond})
	stopM := startNode(t, nodeM)
	defer stopM()

	payload, err := protocol.Encode(protocol.Message{Kind: protocol.KindPingReq, From: "I", Target: "T", SeqNo: 5})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := trI.Send(ctx, "M", payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Wait well past M's RTTTimeout: the only acceptable outcome is a Receive
	// deadline, never a relayed Ack.
	rctx, rcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer rcancel()
	if pkt, err := trI.Receive(rctx); err == nil {
		msg, _ := protocol.Decode(pkt.Payload)
		t.Fatalf("I received %+v, want silence (mediator must not relay on target timeout)", msg)
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
