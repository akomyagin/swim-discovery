// Package swim implements the SWIM protocol cycles. Этап 1 covers the direct
// probe loop: every ProbeInterval pick one random peer, send a Ping, and
// confirm it alive when the matching Ack arrives. Этап 2 piggybacks gossip on
// that same traffic: every outbound Ping/Ack carries a batch of membership
// updates, and every inbound message's batch is merged into the local view.
// Этап 3 adds indirect probing: when the direct Ping stays unanswered, K
// mediators are asked (PingReq) to ping the target on our behalf and relay its
// Ack; only full silence — direct and every indirect — demotes the target to
// Suspect. Suspicion timeouts that escalate Suspect to Dead are Этап 4.
package swim

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/akomyagin/swim-discovery/internal/member"
	"github.com/akomyagin/swim-discovery/internal/protocol"
	"github.com/akomyagin/swim-discovery/internal/transport"
)

// GossipMaxUpdates caps how many membership updates ride on one message, so a
// single datagram stays small and no gossip storm forms. Learning-grade fixed
// value; JSON+UDP on localhost comfortably fits this many Updates.
const GossipMaxUpdates = 6

// Config holds probe-loop timing and randomness. Time stays system-driven
// (context deadlines only); a Clock abstraction arrives in Этап 4.
type Config struct {
	ProbeInterval time.Duration // how often to probe one random peer
	RTTTimeout    time.Duration // how long to wait for a direct Ack
	// IndirectNodes is K: how many random mediators receive a PingReq when the
	// direct Ping goes unanswered. Zero => production default (see NewNode).
	IndirectNodes int
	// IndirectTimeout bounds how long probeOnce waits for ANY relayed Ack after
	// fanning out PingReqs. Zero => production default (see NewNode).
	IndirectTimeout time.Duration
	// Rand is the injectable RNG for peer/mediator selection: tests seed it so
	// the probed target is deterministic instead of global-rand luck.
	Rand *rand.Rand
}

// Node runs the SWIM cycles for one cluster member over a Transport.
type Node struct {
	list *member.List
	tr   transport.Transport
	cfg  Config

	mu    sync.Mutex
	seqNo uint64 // monotonically increasing; correlates Ping/PingReq with Ack

	// pending correlates an outstanding probe's SeqNo with a channel the
	// receive loop signals (non-blocking) when a matching Ack arrives. The
	// prober owns each entry for the probe's whole lifetime — both the direct
	// and the indirect phase — and deletes it exactly once at the end; the
	// receive loop never deletes (see the KindAck case for why).
	pending map[uint64]chan struct{}
}

// NewNode assembles a node. Zero Config fields get production defaults; a nil
// Rand falls back to a time-seeded source (tests inject a fixed seed instead).
func NewNode(list *member.List, tr transport.Transport, cfg Config) *Node {
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if cfg.ProbeInterval == 0 {
		cfg.ProbeInterval = time.Second
	}
	if cfg.RTTTimeout == 0 {
		cfg.RTTTimeout = 300 * time.Millisecond
	}
	if cfg.IndirectNodes == 0 {
		// Classic SWIM k=3; on a small cluster the effective fan-out is capped
		// by the mediators actually available (see pickMediators).
		cfg.IndirectNodes = 3
	}
	if cfg.IndirectTimeout == 0 {
		// The mediator needs its own RTT to the target plus the relay hop back;
		// on localhost/fake the same order as RTTTimeout is enough. Kept as a
		// separate knob so tests can narrow the window independently.
		cfg.IndirectTimeout = cfg.RTTTimeout
	}
	return &Node{
		list:    list,
		tr:      tr,
		cfg:     cfg,
		pending: map[uint64]chan struct{}{},
	}
}

// Run starts the receive and probe loops and blocks until ctx is cancelled and
// every mediator goroutine handlePingReq spawned along the way has returned.
func (n *Node) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		n.receiveLoop(ctx, &wg)
	}()
	go func() {
		defer wg.Done()
		n.probeLoop(ctx)
	}()
	wg.Wait()
}

// toUpdate projects a membership record onto the wire form. It lives in swim
// (not protocol/member) because only this package imports both sides.
func toUpdate(m member.Member) protocol.Update {
	return protocol.Update{
		ID:          m.ID,
		Addr:        m.Addr,
		Incarnation: m.Incarnation,
		State:       m.State,
	}
}

// fromUpdate is toUpdate's inverse: it projects a wire-form rumor back onto a
// membership record for Merge. Kept symmetric with toUpdate so a future field
// added to Update/Member only needs updating in one matched pair.
func fromUpdate(u protocol.Update) member.Member {
	return member.Member{
		ID:          u.ID,
		Addr:        u.Addr,
		Incarnation: u.Incarnation,
		State:       u.State,
	}
}

// collectGossip pulls the next outbound batch from the list and projects it.
// exclude (may be nil) skips records the receiving peer is already known to
// have — see PendingGossip's doc for why that matters.
func (n *Node) collectGossip(exclude map[member.ID]bool) []protocol.Update {
	pending := n.list.PendingGossip(GossipMaxUpdates, exclude)
	if len(pending) == 0 {
		return nil // omitempty keeps the wire clean
	}
	ups := make([]protocol.Update, 0, len(pending))
	for _, m := range pending {
		ups = append(ups, toUpdate(m))
	}
	return ups
}

// applyGossip merges every piggybacked update into the local view and reports
// which IDs it saw, so the caller can avoid echoing them straight back to the
// peer that just supplied them. Records that change get re-queued for gossip
// automatically inside List.Merge — that implicit re-queue is the epidemic:
// an absorbed rumor becomes a candidate for the next outbound message without
// any explicit forwarding step here.
func (n *Node) applyGossip(ups []protocol.Update) map[member.ID]bool {
	if len(ups) == 0 {
		return nil
	}
	seen := make(map[member.ID]bool, len(ups))
	for _, u := range ups {
		n.list.Merge(fromUpdate(u))
		seen[u.ID] = true
	}
	return seen
}

// sendMessage encodes msg and sends it best-effort: failures are logged and
// swallowed, because a lost datagram is a normal SWIM scenario — the
// protocol's timeouts, not the transport, decide what a missing reply means.
func (n *Node) sendMessage(ctx context.Context, addr string, msg protocol.Message) {
	payload, err := protocol.Encode(msg)
	if err != nil {
		log.Printf("swim: encode message (kind=%d) for %s: %v", msg.Kind, addr, err)
		return
	}
	if err := n.tr.Send(ctx, addr, payload); err != nil {
		log.Printf("swim: send (kind=%d) to %s: %v", msg.Kind, addr, err)
	}
}

func (n *Node) receiveLoop(ctx context.Context, wg *sync.WaitGroup) {
	for {
		pkt, err := n.tr.Receive(ctx)
		if err != nil {
			// Receive only fails on ctx cancellation per the Transport
			// contract — time to shut down.
			return
		}
		msg, err := protocol.Decode(pkt.Payload)
		if err != nil {
			// A malformed datagram must never take the node down.
			log.Printf("swim: dropping undecodable packet from %s: %v", pkt.Addr, err)
			continue
		}
		// Absorb piggybacked rumors before dispatching on Kind, so a change
		// carried by this very message can ride back on the Ack we send below.
		seen := n.applyGossip(msg.Updates)
		switch msg.Kind {
		case protocol.KindPing:
			// Echo SeqNo so the sender can correlate; attach the next gossip
			// batch so the reply also spreads rumors. Exclude records this
			// same message just supplied — the sender already has them, so
			// echoing them back is a guaranteed-wasted retransmission.
			n.sendMessage(ctx, pkt.Addr, protocol.Message{
				Kind:    protocol.KindAck,
				From:    n.list.Self(),
				SeqNo:   msg.SeqNo,
				Updates: n.collectGossip(seen),
			})
		case protocol.KindAck:
			// The prober owns pending[seq] for the probe's whole lifetime and
			// deletes it exactly once at the end; deleting here would erase
			// the entry between the direct and indirect phases and lose a
			// relayed Ack. The receive loop only signals, non-blocking.
			n.mu.Lock()
			ch, ok := n.pending[msg.SeqNo]
			n.mu.Unlock()
			if ok {
				select {
				case ch <- struct{}{}:
				default:
					// Buffer already holds a signal: a duplicate Ack (direct
					// arrived late + relayed, or two mediators relayed). The
					// probe needs only one; drop the extra without blocking.
				}
			}
		case protocol.KindPingReq:
			// Mediator role: ping msg.Target ourselves and relay its Ack back
			// to the initiator. The handler blocks up to RTTTimeout waiting
			// for the target, so it runs in its own goroutine — receiveLoop
			// must keep absorbing packets (including Acks for our own probes)
			// meanwhile. Tracked on wg so Run does not return while a mediation
			// is still in flight touching n.pending/n.tr.
			// TODO(Этап 5): unbounded fan-out — a burst of PingReqs (e.g. many
			// simultaneous mediators during a partition) spawns one goroutine
			// each with no cap; fine at localhost scale, revisit once real
			// multi-node partitions are exercised.
			wg.Add(1)
			go func() {
				defer wg.Done()
				n.handlePingReq(ctx, msg, pkt.Addr)
			}()
		}
	}
}

// handlePingReq is the mediator side of indirect probing: on behalf of the
// initiator (msg.From, reachable at initiatorAddr) it pings msg.Target under
// its own fresh SeqNo, and — only if the target answers — relays an Ack to the
// initiator carrying the initiator's ORIGINAL SeqNo, so any Ack on that seq
// closes the initiator's probe. On silence it sends nothing: the protocol has
// no negative reply; the initiator detects failure by its own timeout.
//
// initiatorAddr comes from the transport packet, not from the membership list:
// the mediator may not know the initiator yet (join still converging), while
// the packet's source address is always present.
func (n *Node) handlePingReq(ctx context.Context, msg protocol.Message, initiatorAddr string) {
	// Others() excludes self by construction, so a PingReq asking us to probe
	// ourselves (initiator bug) also falls into the silent-return path.
	var target *member.Member
	for _, m := range n.list.Others() {
		if m.ID == msg.Target {
			mm := m
			target = &mm
			break
		}
	}
	if target == nil {
		return // unknown target: nobody to ping, stay silent
	}

	n.mu.Lock()
	n.seqNo++
	seq := n.seqNo
	ackCh := make(chan struct{}, 1)
	n.pending[seq] = ackCh
	n.mu.Unlock()

	n.sendMessage(ctx, target.Addr, protocol.Message{
		Kind:    protocol.KindPing,
		From:    n.list.Self(),
		SeqNo:   seq,
		Updates: n.collectGossip(nil),
	})
	ok := n.waitAck(ctx, ackCh, n.cfg.RTTTimeout)

	// The mediator owns its nested probe's pending entry, same rule as
	// probeOnce: delete exactly once, here.
	n.mu.Lock()
	delete(n.pending, seq)
	n.mu.Unlock()

	if !ok {
		return // target silent: no relay, the initiator's timeout will tell
	}
	n.sendMessage(ctx, initiatorAddr, protocol.Message{
		Kind:    protocol.KindAck,
		From:    n.list.Self(),
		SeqNo:   msg.SeqNo, // the initiator's end-to-end key, echoed unchanged
		Updates: n.collectGossip(nil),
	})
}

func (n *Node) probeLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.probeOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// waitAck blocks until an Ack lands on ackCh or the timeout elapses (or ctx is
// cancelled). Returns true iff an Ack arrived. It does NOT touch pending — the
// caller owns pending[seq]'s lifetime.
func (n *Node) waitAck(ctx context.Context, ackCh <-chan struct{}, timeout time.Duration) bool {
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-ackCh:
		return true
	case <-wctx.Done():
		return false
	}
}

// pickMediators returns up to IndirectNodes distinct random members to relay a
// PingReq for target, excluding self (Others already does) and target itself
// (asking the target to ping itself proves nothing). Fewer than K come back
// when the cluster is too small; an empty slice means indirect probing is
// impossible (2-node cluster) and the caller skips the indirect phase.
//
// Deliberate simplification: non-Alive candidates are NOT filtered out — a
// Suspect mediator is harmless (it just yields no relay). An Alive-only filter
// is a candidate for Этап 4, once Suspect records actually circulate.
//
// cfg.Rand is not concurrency-safe, so the shuffle runs under n.mu (shared
// with seqNo/pending); Others() is called before taking n.mu so two locks are
// never held at once.
func (n *Node) pickMediators(target member.ID) []member.Member {
	others := n.list.Others()
	candidates := make([]member.Member, 0, len(others))
	for _, m := range others {
		if m.ID == target {
			continue
		}
		candidates = append(candidates, m)
	}
	k := n.cfg.IndirectNodes
	if k > len(candidates) {
		k = len(candidates)
	}
	// Partial Fisher–Yates: shuffle the first k positions, return them.
	n.mu.Lock()
	for i := 0; i < k; i++ {
		j := i + n.cfg.Rand.Intn(len(candidates)-i)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}
	n.mu.Unlock()
	return candidates[:k]
}

// suspect demotes target to StateSuspect at its current incarnation after both
// the direct ping and every indirect ping went unanswered. Merge's equal-
// incarnation precedence (Suspect > Alive) lets the demotion stick and
// re-queues the record for gossip, so the whole cluster learns the suspicion.
// The incarnation is the snapshot probeOnce started from: if the target
// refuted itself with a higher incarnation meanwhile (Этап 4), Merge rejects
// this stale suspicion — which is exactly the desired precedence.
// TODO(Этап 4): arm the suspicion timer here to escalate Suspect->Dead unless
// the target refutes by bumping its incarnation.
func (n *Node) suspect(target member.Member) {
	n.list.Merge(member.Member{
		ID:          target.ID,
		Addr:        target.Addr,
		Incarnation: target.Incarnation, // same incarnation: Suspect outranks Alive
		State:       member.StateSuspect,
	})
}

// probeOnce probes a single random peer in up to two phases sharing one SeqNo
// and one pending channel. Phase 1: direct Ping, wait RTTTimeout. Phase 2 (only
// if phase 1 stayed silent): fan a PingReq out to up to K mediators and wait
// IndirectTimeout for ANY relayed Ack — the mediator echoes our SeqNo, so the
// same channel fires no matter who got through. Only when both phases stay
// silent is the target demoted to Suspect: losing just our own packets to a
// live node must not get it declared dead (the project's central invariant).
func (n *Node) probeOnce(ctx context.Context) {
	others := n.list.Others()
	if len(others) == 0 {
		return // single-node cluster: nobody to probe
	}

	// cfg.Rand (*rand.Rand) is not safe for concurrent use; share n.mu with
	// seqNo/pending rather than add a second lock (pickMediators draws from
	// the same Rand, possibly concurrently with mediator goroutines).
	n.mu.Lock()
	target := others[n.cfg.Rand.Intn(len(others))]
	n.seqNo++
	seq := n.seqNo
	// Buffer of 1 lets receiveLoop signal without close and without blocking.
	ackCh := make(chan struct{}, 1)
	n.pending[seq] = ackCh
	n.mu.Unlock()

	// Phase 1: direct Ping.
	n.sendMessage(ctx, target.Addr, protocol.Message{
		Kind:    protocol.KindPing,
		From:    n.list.Self(),
		SeqNo:   seq,
		Updates: n.collectGossip(nil),
	})
	acked := n.waitAck(ctx, ackCh, n.cfg.RTTTimeout)

	mediators := 0
	if !acked {
		// Phase 2: indirect. Same seq, same channel — a mediator relays the
		// target's Ack back under our SeqNo, so pending[seq] must stay alive
		// across both phases (deleted exactly once, below).
		meds := n.pickMediators(target.ID)
		mediators = len(meds)
		for _, m := range meds {
			n.sendMessage(ctx, m.Addr, protocol.Message{
				Kind:    protocol.KindPingReq,
				From:    n.list.Self(),
				SeqNo:   seq,
				Target:  target.ID,
				Updates: n.collectGossip(nil),
			})
		}
		if mediators > 0 {
			acked = n.waitAck(ctx, ackCh, n.cfg.IndirectTimeout)
		}
		// mediators == 0 (2-node cluster: the only Other IS the target):
		// nobody can probe indirectly, fall through with acked == false.
	}

	// Idempotent cleanup, exactly once, after BOTH phases: probeOnce owns the
	// entry; receiveLoop only signals into it and never deletes.
	n.mu.Lock()
	delete(n.pending, seq)
	n.mu.Unlock()

	if acked {
		// Re-assert liveness at the known incarnation. Merge can legitimately
		// return false here for two different reasons: the record was already
		// Alive (nothing changed), or it was already Suspect at this same
		// incarnation — Merge's Suspect>Alive precedence at equal incarnation
		// (member.go stateRank) then rejects this Ack-driven Alive, and the
		// record stays Suspect. That second case is intentional SWIM
		// semantics carried over from Этап 3: a successful probe from a third
		// party does not clear suspicion, only the target's own higher-
		// incarnation refute does (Этап 4).
		n.list.Merge(member.Member{
			ID:          target.ID,
			Addr:        target.Addr,
			Incarnation: target.Incarnation,
			State:       member.StateAlive,
		})
	} else {
		log.Printf("swim: %s unreachable (direct + %d indirect) — marking suspect", target.ID, mediators)
		n.suspect(target)
	}
}
