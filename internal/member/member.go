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

import "time"

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

// List is a node's local, eventually-consistent view of the cluster.
//
// TODO(Этап 1): implement. Backing store (map[ID]*Member) + mutex, plus the
// merge rule that reconciles an incoming rumor against the local record using
// (Incarnation, State) precedence: higher incarnation always wins; at equal
// incarnation, Dead > Suspect > Alive.
type List struct {
	// TODO(Этап 1): self ID, map[ID]*Member, sync.RWMutex.
}

// NewList creates an empty membership list seeded with the local node.
//
// TODO(Этап 1): implement.
func NewList(self Member) *List {
	_ = self
	panic("TODO(Этап 1): NewList not implemented")
}

// Merge reconciles an incoming view of a member into the local list, applying
// the (Incarnation, State) precedence rule. Returns true if the local view
// changed (and therefore should be re-gossiped).
//
// TODO(Этап 1): implement precedence; TODO(Этап 2): feed changes into gossip.
func (l *List) Merge(m Member) (changed bool) {
	_ = m
	panic("TODO(Этап 1): Merge not implemented")
}

// Members returns a snapshot of the current view for the CLI / probing.
//
// TODO(Этап 1): implement.
func (l *List) Members() []Member {
	panic("TODO(Этап 1): Members not implemented")
}
