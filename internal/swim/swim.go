// Package swim implements the SWIM protocol cycles. Этап 1 covers the direct
// probe loop: every ProbeInterval pick one random peer, send a Ping, and
// confirm it alive when the matching Ack arrives. Этап 2 piggybacks gossip on
// that same traffic: every outbound Ping/Ack carries a batch of membership
// updates, and every inbound message's batch is merged into the local view.
// Этап 3 adds indirect probing: when the direct Ping stays unanswered, K
// mediators are asked (PingReq) to ping the target on our behalf and relay its
// Ack; only full silence — direct and every indirect — demotes the target to
// Suspect. Этап 4 closes the failure-detection loop: a Suspect that goes
// unrefuted for SuspicionTimeout is escalated to Dead, and a node that hears
// a Suspect/Dead rumor about ITSELF refutes it by bumping its incarnation.
// All probe/suspicion timing goes through an injectable Clock so tests drive
// it deterministically.
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

// Clock abstracts time for every probe/suspicion timeout in this package, so
// tests can drive deadlines deterministically (fakeClock.Advance) instead of
// sleeping on real timers. Production uses systemClock.
type Clock interface {
	// Now returns the current time. Stamps and diagnostics only — protocol
	// decisions use the relative mechanisms below, never wall-clock compares.
	Now() time.Time
	// After returns a channel that receives the time once d has elapsed.
	// Used for in-line waits (Ack timeouts in waitAck).
	After(d time.Duration) <-chan time.Time
	// AfterFunc schedules f to run once d has elapsed and returns a handle
	// that can cancel it. Used for suspicion timers.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a cancellable pending AfterFunc. Stop reports whether it prevented
// the callback from running (false: already fired or already stopped) — the
// same contract as time.Timer.Stop.
type Timer interface {
	Stop() bool
}

// systemClock is the production Clock: thin proxies over package time.
type systemClock struct{}

func (systemClock) Now() time.Time                            { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time    { return time.After(d) }
func (systemClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

var _ Clock = systemClock{} // compile-time contract, mirrors the transport adapters

// Config holds probe/suspicion timing and randomness.
type Config struct {
	ProbeInterval time.Duration // how often to probe one random peer
	RTTTimeout    time.Duration // how long to wait for a direct Ack
	// IndirectNodes is K: how many random mediators receive a PingReq when the
	// direct Ping goes unanswered. Zero => production default (see NewNode).
	IndirectNodes int
	// IndirectTimeout bounds how long probeOnce waits for ANY relayed Ack after
	// fanning out PingReqs. Zero => production default (see NewNode).
	IndirectTimeout time.Duration
	// SuspicionTimeout is how long a Suspect may stay unrefuted before this
	// node declares it Dead. Zero => production default (see NewNode). Project
	// invariant: RTTTimeout, IndirectTimeout << SuspicionTimeout, so the
	// indirect-probing rescue (Этап 3) always gets to finish long before the
	// suspicion deadline can possibly fire.
	SuspicionTimeout time.Duration
	// DeadTimeout is the grace period a Dead record lingers before this node
	// evicts it from the list entirely. Zero => production default (see NewNode).
	// Ordered ABOVE SuspicionTimeout: a node lives in the list as Dead at least
	// as long as it lived as Suspect, so the death rumor has time to spread by
	// gossip before the record is forgotten (evicting sooner would erase it
	// before peers learn of the death). See member.List.Evict.
	DeadTimeout time.Duration
	// Clock drives every probe/suspicion timeout. Nil => systemClock; tests
	// inject fakeClock to advance time deterministically.
	Clock Clock
	// Rand is the injectable RNG for peer/mediator selection: tests seed it so
	// the probed target is deterministic instead of global-rand luck.
	Rand *rand.Rand
}

// Node runs the SWIM cycles for one cluster member over a Transport.
type Node struct {
	list  *member.List
	tr    transport.Transport
	cfg   Config
	clock Clock // cached cfg.Clock: every timeout in this package goes through it

	mu    sync.Mutex
	seqNo uint64 // monotonically increasing; correlates Ping/PingReq with Ack

	// pending correlates an outstanding probe's SeqNo with a channel the
	// receive loop signals (non-blocking) when a matching Ack arrives. The
	// prober owns each entry for the probe's whole lifetime — both the direct
	// and the indirect phase — and deletes it exactly once at the end; the
	// receive loop never deletes (see the KindAck case for why).
	pending map[uint64]chan struct{}

	// suspicions holds the armed Suspect→Dead escalation timers, one per
	// target, each valid only for the incarnation it was armed at. Guarded by
	// its own mutex rather than n.mu so timer bookkeeping never extends the
	// hot seqNo/pending/Rand critical section (and so escalation callbacks —
	// which fire from Clock goroutines — contend only with each other).
	suspMu     sync.Mutex
	suspicions map[member.ID]*suspicionTimer

	// evictions holds the armed Dead→evicted timers, one per target, each valid
	// only for the incarnation it was armed at (mirrors suspicions). Guarded by
	// its own evictMu, same rationale as suspMu: eviction bookkeeping stays out
	// of the hot seqNo/pending/Rand section, and Clock-goroutine callbacks
	// contend only with each other.
	evictMu   sync.Mutex
	evictions map[member.ID]*evictionTimer
}

// suspicionTimer is one armed Suspect→Dead escalation, valid only for the
// incarnation the target was suspected at: any information at a higher
// incarnation (a refute above all) obsoletes and cancels it.
type suspicionTimer struct {
	incarnation uint64
	timer       Timer
}

// evictionTimer is one armed Dead→evicted removal, valid only for the
// incarnation the target was declared Dead at: a revive at a higher
// incarnation obsoletes and cancels it (mirrors suspicionTimer).
type evictionTimer struct {
	incarnation uint64
	timer       Timer
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
	if cfg.SuspicionTimeout == 0 {
		// Deliberately orders of magnitude above RTTTimeout+IndirectTimeout
		// (seconds vs hundreds of ms): the indirect-probing rescue must always
		// finish long before a suspicion can escalate, or the project's central
		// invariant (packet loss to a live node must not kill it) breaks.
		cfg.SuspicionTimeout = 5 * time.Second
	}
	if cfg.DeadTimeout == 0 {
		// Above SuspicionTimeout (5s): the Dead rumor must fully disseminate
		// before the record is forgotten. See member.List.Evict and the timeout
		// ladder note above (RTT+Indirect << Suspicion < Dead).
		cfg.DeadTimeout = 10 * time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = systemClock{}
	}
	return &Node{
		list:       list,
		tr:         tr,
		cfg:        cfg,
		clock:      cfg.Clock,
		pending:    map[uint64]chan struct{}{},
		suspicions: map[member.ID]*suspicionTimer{},
		evictions:  map[member.ID]*evictionTimer{},
	}
}

// Run starts the receive and probe loops and blocks until ctx is cancelled and
// every mediator goroutine handlePingReq spawned along the way has returned.
// On the way out it disarms every pending suspicion timer, so no escalation
// outlives the node's ctx (an AfterFunc callback already in flight at that
// exact moment is harmless: it only touches the mutex-guarded suspicions map
// and List, both of which outlive Run).
func (n *Node) Run(ctx context.Context) {
	defer n.stopSuspicionTimers()
	defer n.stopEvictionTimers()
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
// StateChangedAt is deliberately NOT projected: suspicion timing is each
// observer's local concern (every node arms its own timer from when IT saw
// the Suspect), so the wire stays timestamp-free — see protocol.Update.
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
//
// Этап 4 adds two side channels. A Suspect/Dead rumor about SELF triggers
// refute (maybeRefute); self is then deliberately NOT marked seen — after a
// refute our record differs from what the peer sent, and suppressing it from
// the reply would delay spreading the correction on the very Ack answering
// the slur. For everyone else, a freshly-absorbed Suspect arms this node's
// own local suspicion timer (SWIM: every node that hears a Suspect times it
// independently), and fresh Alive information cancels an obsolete one. Этап 6
// adds a third: a freshly-absorbed Dead also arms this node's own eviction
// timer (gossip-delivered deaths must eventually be forgotten, not only
// locally-escalated ones), and Alive cancels that too — see armEviction/
// cancelEviction below.
func (n *Node) applyGossip(ups []protocol.Update) map[member.ID]bool {
	if len(ups) == 0 {
		return nil
	}
	self := n.list.Self()
	seen := make(map[member.ID]bool, len(ups))
	for _, u := range ups {
		if u.ID == self {
			// We are the authority on ourselves: never merge rumors about
			// self, only refute the bad ones.
			n.maybeRefute(u)
			continue
		}
		m := fromUpdate(u)
		changed := n.list.Merge(m)
		seen[u.ID] = true
		if !changed {
			continue // rejected by precedence: must neither arm nor cancel
		}
		switch u.State {
		case member.StateSuspect:
			n.armSuspicion(m)
		case member.StateDead:
			n.cancelSuspicion(u.ID, u.Incarnation, u.State)
			// A death learned by rumor (not only by our own escalation) must
			// also start THIS node's grace timer, or a gossip-delivered Dead
			// would linger forever. armEviction is idempotent per (ID, inc),
			// so repeated Dead rumors never double a timer.
			n.armEviction(m)
		case member.StateAlive:
			n.cancelSuspicion(u.ID, u.Incarnation, u.State)
			n.cancelEviction(u.ID, u.Incarnation, u.State)
		}
	}
	return seen
}

// maybeRefute answers a Suspect/Dead rumor about self: bump our incarnation
// past the rumor's and re-announce ourselves Alive — by Merge precedence the
// higher incarnation vanquishes the rumor everywhere it has spread (the
// re-queue inside Merge puts the refutation on the next outbound piggyback;
// the epidemic does the rest, no dedicated broadcast needed).
//
// Anti-storm guard: only a rumor at incarnation >= ours triggers a bump. A
// stale rumor (lower incarnation) already loses in everyone's Merge, and
// bumping on it — or on harmless Alive mentions — would escalate incarnation
// numbers without bound.
func (n *Node) maybeRefute(u protocol.Update) {
	if u.State == member.StateAlive {
		return
	}
	self, ok := n.list.Get(n.list.Self())
	if !ok || u.Incarnation < self.Incarnation {
		return
	}
	// u.Incarnation >= ours here, so +1 tops both the rumor and our record.
	newInc := u.Incarnation + 1
	log.Printf("swim: refuting %v rumor about self — alive at incarnation %d", u.State, newInc)
	n.list.Merge(member.Member{
		ID:          self.ID,
		Addr:        self.Addr,
		Incarnation: newInc,
		State:       member.StateAlive,
	})
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

	// The mediator owns its nested probe's pending entry, same rule as
	// probeOnce: register once, delete exactly once.
	seq, ackCh := n.registerProbe()
	ok := n.directPing(ctx, target.Addr, seq, ackCh)
	n.finishProbe(seq)

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

// probeLoop paces probeOnce. The ticker deliberately stays on real time, NOT
// on Clock: it is the loop's scheduler, not a protocol timeout — no test
// asserts on when the next probe fires (they call probeOnce directly and park
// the ticker with a huge ProbeInterval), while every deadline a test DOES
// depend on (RTT/indirect/suspicion) goes through Clock.
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

// registerProbe allocates a fresh SeqNo and registers its pending Ack channel
// (buffered 1, so receiveLoop signals without close and without blocking).
// The caller becomes the entry's owner: it must release it with finishProbe
// exactly once, after every phase that could still use the channel is over.
func (n *Node) registerProbe() (uint64, chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seqNo++
	ch := make(chan struct{}, 1)
	n.pending[n.seqNo] = ch
	return n.seqNo, ch
}

// finishProbe releases pending[seq] — the owner's single delete; receiveLoop
// only ever signals into the channel and never deletes (see KindAck).
func (n *Node) finishProbe(seq uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.pending, seq)
}

// directPing is the shared direct-probe primitive of probeOnce's phase 1 and
// the mediator's nested probe in handlePingReq: send a Ping to addr under an
// already-registered seq and wait for the matching Ack. The direct-ping
// timeout (RTTTimeout, via Clock) is defined here ONCE so both callers always
// agree on it. It does not touch pending — the caller owns pending[seq]'s
// lifetime via registerProbe/finishProbe, which is what lets probeOnce keep
// the same seq and channel alive across its indirect phase.
func (n *Node) directPing(ctx context.Context, addr string, seq uint64, ackCh <-chan struct{}) bool {
	n.sendMessage(ctx, addr, protocol.Message{
		Kind:    protocol.KindPing,
		From:    n.list.Self(),
		SeqNo:   seq,
		Updates: n.collectGossip(nil),
	})
	return n.waitAck(ctx, ackCh, n.cfg.RTTTimeout)
}

// waitAck blocks until an Ack lands on ackCh, the Clock-driven timeout
// elapses, or ctx is cancelled (shutdown). Returns true iff an Ack arrived.
// It does NOT touch pending — the caller owns pending[seq]'s lifetime.
func (n *Node) waitAck(ctx context.Context, ackCh <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-ackCh:
		return true
	case <-n.clock.After(timeout):
		return false
	case <-ctx.Done():
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
// remains a candidate for Этап 5, once realistic partitions are exercised.
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
// refuted itself with a higher incarnation meanwhile, Merge rejects this
// stale suspicion — which is exactly the desired precedence.
//
// Этап 4: entering Suspect arms the escalation timer — unrefuted for
// SuspicionTimeout means Dead. The timer is armed at the incarnation actually
// recorded in the list, not the probe's snapshot: the record may already be
// Suspect at a newer incarnation via gossip, and armSuspicion is idempotent
// per (ID, incarnation), so a re-suspicion never doubles a timer. If the
// target already moved past Suspect (Dead, or refuted to a higher-incarnation
// Alive), there is nothing to arm.
func (n *Node) suspect(target member.Member) {
	n.list.Merge(member.Member{
		ID:          target.ID,
		Addr:        target.Addr,
		Incarnation: target.Incarnation, // same incarnation: Suspect outranks Alive
		State:       member.StateSuspect,
	})
	if rec, ok := n.list.Get(target.ID); ok && rec.State == member.StateSuspect {
		n.armSuspicion(rec)
	}
}

// armSuspicion arms the Suspect→Dead escalation timer for target, valid for
// target.Incarnation. Idempotent per (ID, incarnation): a repeat suspicion at
// the same (or an older) incarnation is a no-op, so escalation can fire at
// most once per incarnation. A suspicion at a HIGHER incarnation replaces the
// armed timer — the old one is obsolete by precedence, and the fresh Suspect
// deserves its full timeout.
func (n *Node) armSuspicion(target member.Member) {
	n.suspMu.Lock()
	defer n.suspMu.Unlock()
	if cur, ok := n.suspicions[target.ID]; ok {
		if cur.incarnation >= target.Incarnation {
			return
		}
		cur.timer.Stop()
	}
	id, addr, inc := target.ID, target.Addr, target.Incarnation
	n.suspicions[id] = &suspicionTimer{
		incarnation: inc,
		timer: n.clock.AfterFunc(n.cfg.SuspicionTimeout, func() {
			n.escalate(id, addr, inc)
		}),
	}
}

// cancelSuspicion disarms target's timer when fresh information supersedes
// the suspicion: Alive at a higher incarnation is the target's refute; Dead
// at the same or a higher incarnation means someone else already escalated.
// Anything weaker (notably Alive at the SAME incarnation — a successful
// re-probe by us or a third party) deliberately does NOT disarm: per SWIM
// only the target's own incarnation bump clears a suspicion.
func (n *Node) cancelSuspicion(id member.ID, inc uint64, state member.State) {
	n.suspMu.Lock()
	defer n.suspMu.Unlock()
	cur, ok := n.suspicions[id]
	if !ok {
		return
	}
	refuted := state == member.StateAlive && inc > cur.incarnation
	confirmed := state == member.StateDead && inc >= cur.incarnation
	if !refuted && !confirmed {
		return
	}
	cur.timer.Stop()
	delete(n.suspicions, id)
}

// escalate is the suspicion timer's payload: no refutation arrived within
// SuspicionTimeout, declare the target Dead. The Merge (same incarnation,
// Dead > Suspect) both applies the verdict and re-queues it for gossip, so
// the death spreads like any membership change. If the target moved to a
// higher incarnation meanwhile (a refute racing the timer), Merge rejects the
// stale Dead — precedence, not the timer, always has the final word, which is
// why this must go through Merge and never write the list directly.
func (n *Node) escalate(id member.ID, addr string, inc uint64) {
	n.suspMu.Lock()
	cur, ok := n.suspicions[id]
	if !ok || cur.incarnation != inc {
		// Disarmed or re-armed at a newer incarnation between the timer
		// firing and this callback running: not ours to escalate anymore.
		n.suspMu.Unlock()
		return
	}
	delete(n.suspicions, id)
	n.suspMu.Unlock()

	if n.list.Merge(member.Member{
		ID:          id,
		Addr:        addr,
		Incarnation: inc, // same incarnation: Dead outranks Suspect
		State:       member.StateDead,
	}) {
		log.Printf("swim: %s unrefuted for %v — declaring dead (incarnation %d)", id, n.cfg.SuspicionTimeout, inc)
		// The target just became Dead in our local view: start the grace
		// period after which the record is forgotten entirely.
		n.armEviction(member.Member{ID: id, Addr: addr, Incarnation: inc})
	}
}

// armEviction arms the Dead→evicted removal timer for target, valid for
// target.Incarnation. Idempotent per (ID, incarnation): a repeat at the same
// (or older) incarnation is a no-op, so eviction fires at most once per
// incarnation. A Dead at a HIGHER incarnation replaces the armed timer (the
// old one is obsolete by precedence). Mirror of armSuspicion.
func (n *Node) armEviction(target member.Member) {
	n.evictMu.Lock()
	defer n.evictMu.Unlock()
	if cur, ok := n.evictions[target.ID]; ok {
		if cur.incarnation >= target.Incarnation {
			return
		}
		cur.timer.Stop()
	}
	id, inc := target.ID, target.Incarnation
	n.evictions[id] = &evictionTimer{
		incarnation: inc,
		timer: n.clock.AfterFunc(n.cfg.DeadTimeout, func() {
			n.evict(id, inc)
		}),
	}
}

// cancelEviction disarms target's eviction timer when the record leaves Dead:
// a revive (Alive) at a HIGHER incarnation resurrects the node, so it must no
// longer be evicted. (A Dead at a higher incarnation is handled by armEviction
// re-arming.) Mirror of cancelSuspicion, but the trigger is "no longer
// Dead@inc". A Suspect at a higher incarnation deliberately does NOT disarm:
// the node is under suspicion again and must not be forgotten by the old
// timer — arm/cancel stay symmetric with the suspicion logic.
func (n *Node) cancelEviction(id member.ID, inc uint64, state member.State) {
	n.evictMu.Lock()
	defer n.evictMu.Unlock()
	cur, ok := n.evictions[id]
	if !ok {
		return
	}
	if state != member.StateAlive || inc <= cur.incarnation {
		return
	}
	cur.timer.Stop()
	delete(n.evictions, id)
}

// evict is the eviction timer's payload: the grace period elapsed with the
// target still Dead, so remove it from the list. Guard mirrors escalate: if
// the map entry was disarmed or re-armed at a newer incarnation between the
// timer firing and this callback, it is not ours to evict. The actual removal
// goes through member.List.Evict, which re-checks Dead@inc under the list lock
// (a revive racing the timer then makes Evict a no-op — precedence has the
// final word, same as escalate → Merge).
func (n *Node) evict(id member.ID, inc uint64) {
	n.evictMu.Lock()
	cur, ok := n.evictions[id]
	if !ok || cur.incarnation != inc {
		n.evictMu.Unlock()
		return
	}
	delete(n.evictions, id)
	n.evictMu.Unlock()

	if n.list.Evict(id, inc) {
		log.Printf("swim: evicting %s after %v dead (incarnation %d)", id, n.cfg.DeadTimeout, inc)
	}
}

// stopSuspicionTimers disarms every pending suspicion timer; Run defers it so
// no escalation outlives the node's ctx.
func (n *Node) stopSuspicionTimers() {
	n.suspMu.Lock()
	defer n.suspMu.Unlock()
	for id, cur := range n.suspicions {
		cur.timer.Stop()
		delete(n.suspicions, id)
	}
}

// stopEvictionTimers disarms every pending eviction timer; Run defers it so no
// eviction outlives the node's ctx (mirror of stopSuspicionTimers).
func (n *Node) stopEvictionTimers() {
	n.evictMu.Lock()
	defer n.evictMu.Unlock()
	for id, cur := range n.evictions {
		cur.timer.Stop()
		delete(n.evictions, id)
	}
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
	targets := make([]member.Member, 0, len(others))
	for _, m := range others {
		if m.State == member.StateDead {
			// A Dead record is being gossiped out and will be evicted shortly;
			// probing it only yields timeout noise (Этап 6). Suspect targets
			// stay in — they are still under active probing by design.
			continue
		}
		targets = append(targets, m)
	}
	if len(targets) == 0 {
		return // nobody probeable (single node, or every peer already Dead)
	}

	// cfg.Rand (*rand.Rand) is not safe for concurrent use; share n.mu with
	// seqNo/pending rather than add a second lock (pickMediators draws from
	// the same Rand, possibly concurrently with mediator goroutines).
	n.mu.Lock()
	target := targets[n.cfg.Rand.Intn(len(targets))]
	n.mu.Unlock()

	seq, ackCh := n.registerProbe()

	// Phase 1: direct Ping — the same primitive the mediator uses, so the
	// direct timeout lives in exactly one place (directPing).
	acked := n.directPing(ctx, target.Addr, seq, ackCh)

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

	// Cleanup exactly once, after BOTH phases: probeOnce owns the entry;
	// receiveLoop only signals into it and never deletes.
	n.finishProbe(seq)

	if acked {
		// Re-assert liveness at the known incarnation. Merge can legitimately
		// return false here for two different reasons: the record was already
		// Alive (nothing changed), or it was already Suspect at this same
		// incarnation — Merge's Suspect>Alive precedence at equal incarnation
		// (member.go stateRank) then rejects this Ack-driven Alive, and the
		// record stays Suspect. That second case is intentional SWIM
		// semantics: a successful probe from a third party does not clear
		// suspicion (nor disarm its timer) — only the target's own higher-
		// incarnation refute does (see maybeRefute/cancelSuspicion).
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
