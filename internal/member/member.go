// Package member describes cluster nodes and the eventually-consistent
// membership list that every node maintains locally.
//
// Design notes (SWIM):
//   - Each node keeps its own view of the whole cluster. Views converge over
//     time via gossip; they are never guaranteed globally consistent at any
//     single instant (eventual consistency).
//   - Membership updates carry an incarnation number so a node can refute a
//     stale "suspect"/"dead" rumor about itself by re-broadcasting a higher
//     incarnation. This is the core anti-false-positive mechanism at the
//     membership-list level (see internal/protocol for the wire messages).
package member

import (
	"math"
	"sort"
	"sync"
	"time"
)

// State is the SWIM lifecycle state of a node as seen by a peer.
type State uint8

const (
	// StateAlive — node is believed healthy.
	StateAlive State = iota
	// StateSuspect — a direct probe failed; not yet declared dead. Indirect
	// probing and a suspicion timeout decide the outcome (Этап 3, Этап 4).
	StateSuspect
	// StateDead — declared failed after the suspicion timeout expired without
	// refutation.
	StateDead
)

// String renders the state for logs and the CLI.
func (s State) String() string {
	switch s {
	case StateAlive:
		return "alive"
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// ID uniquely identifies a node within the cluster. In v1 it is the node's
// "host:port" UDP address on localhost.
type ID string

// Member is one node as recorded in a peer's membership list.
type Member struct {
	ID   ID
	Addr string // UDP address, e.g. "127.0.0.1:7947"
	// Incarnation is bumped by the node itself to refute suspicion. A rumor
	// with a lower incarnation than what we know is ignored.
	Incarnation uint64
	State       State
	// StateChangedAt marks when State last transitioned; drives the suspicion
	// timeout in Этап 4.
	StateChangedAt time.Time
}

// List is a node's local, eventually-consistent view of the cluster. It
// reconciles incoming rumors against local records using (Incarnation, State)
// precedence: higher incarnation always wins; at equal incarnation,
// Dead > Suspect > Alive.
type List struct {
	self    ID
	mu      sync.RWMutex
	members map[ID]*Member
	// gossipTx tracks the remaining re-broadcasts per member's latest change.
	// It lives here, under the same mutex as members, because gossip candidates
	// are exactly the records Merge changes — a separate queue would race
	// between "Merge marked it fresh" and "PendingGossip decremented it".
	gossipTx map[ID]int
}

// NewList creates a membership list seeded with the local node.
func NewList(self Member) *List {
	l := &List{
		self:     self.ID,
		members:  map[ID]*Member{},
		gossipTx: map[ID]int{},
	}
	// Store a private copy so callers cannot mutate the record past the mutex.
	cp := self
	l.members[self.ID] = &cp
	// Announce self on the first outbound messages: without it a joining node
	// stays invisible until a seed happens to probe it, which the seed cannot
	// do before learning about it. The budget caps the announcement — self
	// leaves the queue once spent, so there is no endless self-gossip.
	l.gossipTx[self.ID] = gossipRetransmitBudget(len(l.members))
	return l
}

// stateRank orders states for equal-incarnation conflicts: the "worse" state
// wins so that a suspicion/death rumor cannot be silently shadowed by a stale
// alive record — only a higher incarnation (refute) overrides it.
func stateRank(s State) int {
	switch s {
	case StateSuspect:
		return 1
	case StateDead:
		return 2
	default:
		return 0
	}
}

// gossipRetransmitBudget returns how many times a single membership change
// should be re-broadcast: 3·ceil(log2(N+1)), N = known members (always ≥1,
// self included, so the result is never below 3). The logarithmic law is the
// standard gossip dissemination bound (rounds-to-cover grows ~log N). The 3×
// is the λ multiplier of classic SWIM and is load-bearing, not tuning:
// transmissions land on random peers that often already know the rumor (on a
// sparse join a node may know a single peer and burn its whole budget on it),
// so without the multiplier rumors measurably go extinct before covering a
// 7-node ring (9/10 seed sets failed to converge; 0/50 with λ=3).
func gossipRetransmitBudget(n int) int {
	return 3 * int(math.Ceil(math.Log2(float64(n+1))))
}

// Merge reconciles an incoming view of a member into the local list, applying
// the (Incarnation, State) precedence rule. Returns true if the local view
// changed (and therefore should be re-gossiped): every change re-arms the
// record's full retransmit budget, feeding the gossip queue.
func (l *List) Merge(m Member) (changed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cur, ok := l.members[m.ID]
	if ok {
		switch {
		case m.Incarnation > cur.Incarnation:
			// incoming wins
		case m.Incarnation < cur.Incarnation:
			return false
		default:
			if stateRank(m.State) <= stateRank(cur.State) {
				return false
			}
		}
	}

	cp := m
	if cp.StateChangedAt.IsZero() && (!ok || cur.State != m.State) {
		// Transition without an explicit timestamp from the rumor — covers
		// both a local state change and a first-ever insert (which is a
		// transition from "unknown" to whatever state the rumor carries).
		// Stamp now so Этап 4 suspicion timeouts have a starting point.
		cp.StateChangedAt = time.Now()
	}
	l.members[m.ID] = &cp
	// Re-arm the full budget even if the record was already queued: the latest
	// change supersedes whatever remained of the previous one. Computed after
	// the insert so len includes the member just added.
	l.gossipTx[m.ID] = gossipRetransmitBudget(len(l.members))
	return true
}

// PendingGossip returns up to limit membership records that still need to be
// disseminated, least-spread first (largest remaining retransmit budget), and
// decrements each returned record's budget. A record whose budget reaches zero
// is dropped from the gossip queue until Merge marks it changed again. Returns
// a snapshot (copies), safe to encode after the lock is released.
//
// exclude skips candidates whose ID is set, without spending their budget:
// echoing a record straight back to the peer that just supplied it is a
// guaranteed-wasted retransmission (they already have it by definition), and
// on a sparse cluster that waste can exhaust a record's budget before it ever
// reaches an uninformed peer, permanently stalling its dissemination. Pass
// nil when there is nothing to exclude (e.g. an outbound probe Ping).
func (l *List) PendingGossip(limit int, exclude map[ID]bool) []Member {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := []Member{}
	if limit <= 0 {
		return out
	}

	ids := make([]ID, 0, len(l.gossipTx))
	for id := range l.gossipTx {
		if exclude[id] {
			continue
		}
		ids = append(ids, id)
	}
	// Least-spread first: a larger remaining budget means fewer broadcasts so
	// far, so fresh rumors do not starve behind well-traveled ones. The ID
	// tie-break keeps the order deterministic for tests.
	sort.Slice(ids, func(i, j int) bool {
		if l.gossipTx[ids[i]] != l.gossipTx[ids[j]] {
			return l.gossipTx[ids[i]] > l.gossipTx[ids[j]]
		}
		return ids[i] < ids[j]
	})

	if len(ids) > limit {
		ids = ids[:limit]
	}
	for _, id := range ids {
		out = append(out, *l.members[id])
		l.gossipTx[id]--
		if l.gossipTx[id] <= 0 {
			delete(l.gossipTx, id)
		}
	}
	return out
}

// Members returns a sorted snapshot of the current view for the CLI / probing.
func (l *List) Members() []Member {
	return l.snapshot(false)
}

// Self returns this node's own ID.
func (l *List) Self() ID {
	return l.self
}

// Others returns a snapshot of all members except self (any state), sorted by
// ID. Probing must target peers, never self, so the filter lives here rather
// than being repeated in the swim core.
func (l *List) Others() []Member {
	return l.snapshot(true)
}

// snapshot copies the current view, optionally excluding self, sorted by ID
// for deterministic tests and stable CLI output.
func (l *List) snapshot(skipSelf bool) []Member {
	l.mu.RLock()
	defer l.mu.RUnlock()

	out := make([]Member, 0, len(l.members))
	for id, m := range l.members {
		if skipSelf && id == l.self {
			continue
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
