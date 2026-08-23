package member

import (
	"reflect"
	"testing"
)

// TestList_Merge exercises the (Incarnation, State) precedence rule — the
// load-bearing invariant of the membership list.
func TestList_Merge(t *testing.T) {
	tests := []struct {
		name        string
		local       *Member // nil = no existing record
		incoming    Member
		wantChanged bool
		wantState   State
		wantInc     uint64
	}{
		{
			name:        "new ID inserts",
			local:       nil,
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 3, State: StateSuspect},
			wantChanged: true,
			wantState:   StateSuspect,
			wantInc:     3,
		},
		{
			name:        "higher incarnation wins regardless of state",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 2, State: StateDead},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive},
			wantChanged: true,
			wantState:   StateAlive,
			wantInc:     5,
		},
		{
			name:        "lower incarnation ignored",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 4, State: StateDead},
			wantChanged: false,
			wantState:   StateAlive,
			wantInc:     5,
		},
		{
			name:        "equal inc: suspect beats alive",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateSuspect},
			wantChanged: true,
			wantState:   StateSuspect,
			wantInc:     5,
		},
		{
			name:        "equal inc: dead beats alive",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateDead},
			wantChanged: true,
			wantState:   StateDead,
			wantInc:     5,
		},
		{
			name:        "equal inc: dead beats suspect",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateSuspect},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateDead},
			wantChanged: true,
			wantState:   StateDead,
			wantInc:     5,
		},
		{
			name:        "equal inc: alive does not displace suspect",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateSuspect},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive},
			wantChanged: false,
			wantState:   StateSuspect,
			wantInc:     5,
		},
		{
			name:        "equal inc: suspect does not displace dead",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateDead},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateSuspect},
			wantChanged: false,
			wantState:   StateDead,
			wantInc:     5,
		},
		{
			name:        "equal inc: alive over alive is a no-op",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive},
			wantChanged: false,
			wantState:   StateAlive,
			wantInc:     5,
		},
		{
			name:        "equal inc: dead over dead is a no-op",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateDead},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 5, State: StateDead},
			wantChanged: false,
			wantState:   StateDead,
			wantInc:     5,
		},
		{
			name:        "refute-like: alive@6 displaces suspect@5",
			local:       &Member{ID: "B", Addr: "B", Incarnation: 5, State: StateSuspect},
			incoming:    Member{ID: "B", Addr: "B", Incarnation: 6, State: StateAlive},
			wantChanged: true,
			wantState:   StateAlive,
			wantInc:     6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewList(Member{ID: "self", Addr: "self", State: StateAlive})
			if tt.local != nil {
				if !l.Merge(*tt.local) {
					t.Fatalf("setup Merge(%+v) = false, want true", *tt.local)
				}
			}

			if got := l.Merge(tt.incoming); got != tt.wantChanged {
				t.Errorf("Merge changed = %v, want %v", got, tt.wantChanged)
			}

			var found *Member
			for _, m := range l.Others() {
				if m.ID == tt.incoming.ID {
					mm := m
					found = &mm
					break
				}
			}
			if found == nil {
				t.Fatalf("member %q missing after Merge", tt.incoming.ID)
			}
			if found.State != tt.wantState {
				t.Errorf("state = %v, want %v", found.State, tt.wantState)
			}
			if found.Incarnation != tt.wantInc {
				t.Errorf("incarnation = %d, want %d", found.Incarnation, tt.wantInc)
			}
		})
	}
}

func TestNewList_SeedsSelf(t *testing.T) {
	self := Member{ID: "A", Addr: "A", State: StateAlive}
	l := NewList(self)

	if got := l.Self(); got != self.ID {
		t.Errorf("Self() = %q, want %q", got, self.ID)
	}
	members := l.Members()
	if len(members) != 1 || members[0].ID != self.ID {
		t.Errorf("Members() = %+v, want just self", members)
	}
	if others := l.Others(); len(others) != 0 {
		t.Errorf("Others() = %+v, want empty", others)
	}
}

func TestList_Others_ExcludesSelf(t *testing.T) {
	l := NewList(Member{ID: "B", Addr: "B", State: StateAlive})
	l.Merge(Member{ID: "C", Addr: "C", State: StateAlive})
	l.Merge(Member{ID: "A", Addr: "A", State: StateAlive})

	others := l.Others()
	gotIDs := make([]ID, 0, len(others))
	for _, m := range others {
		gotIDs = append(gotIDs, m.ID)
	}
	if want := []ID{"A", "C"}; !reflect.DeepEqual(gotIDs, want) {
		t.Errorf("Others() IDs = %v, want %v (self excluded, sorted)", gotIDs, want)
	}
}

// drainGossip exhausts the gossip queue so tests can arrange a known-empty
// starting point before arming specific records.
func drainGossip(t *testing.T, l *List) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if len(l.PendingGossip(100, nil)) == 0 {
			return
		}
	}
	t.Fatal("gossip queue did not drain in 100 rounds")
}

func TestList_PendingGossip_SeedsSelf(t *testing.T) {
	self := Member{ID: "A", Addr: "A", State: StateAlive}
	l := NewList(self)

	got := l.PendingGossip(10, nil)
	if len(got) != 1 {
		t.Fatalf("PendingGossip(10, nil) = %+v, want exactly self", got)
	}
	if got[0].ID != self.ID || got[0].Addr != self.Addr || got[0].State != StateAlive {
		t.Errorf("PendingGossip self = %+v, want %+v", got[0], self)
	}
}

func TestList_PendingGossip_BudgetDecrements(t *testing.T) {
	l := NewList(Member{ID: "A", Addr: "A", State: StateAlive})

	// N=1 → budget floor 3: self must be returned exactly three times.
	for i := 0; i < 3; i++ {
		got := l.PendingGossip(10, nil)
		if len(got) != 1 || got[0].ID != "A" {
			t.Fatalf("call %d: PendingGossip(10, nil) = %+v, want self", i+1, got)
		}
	}
	if got := l.PendingGossip(10, nil); len(got) != 0 {
		t.Errorf("4th PendingGossip = %+v, want empty (budget spent)", got)
	}
}

func TestList_PendingGossip_ZeroLimit(t *testing.T) {
	l := NewList(Member{ID: "A", Addr: "A", State: StateAlive})

	if got := l.PendingGossip(0, nil); len(got) != 0 {
		t.Errorf("PendingGossip(0, nil) = %+v, want empty", got)
	}
	// The guard must not have decremented anything: self still queued 3 times.
	for i := 0; i < 3; i++ {
		if got := l.PendingGossip(10, nil); len(got) != 1 {
			t.Fatalf("call %d after zero-limit: got %+v, want self", i+1, got)
		}
	}
}

func TestList_Merge_QueuesChanged(t *testing.T) {
	l := NewList(Member{ID: "A", Addr: "A", State: StateAlive})
	drainGossip(t, l)

	if !l.Merge(Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive}) {
		t.Fatal("Merge(B) = false, want true")
	}
	got := l.PendingGossip(10, nil)
	if len(got) != 1 || got[0].ID != "B" {
		t.Errorf("PendingGossip after Merge(B) = %+v, want just B", got)
	}
}

func TestList_Merge_NoChange_NoQueue(t *testing.T) {
	l := NewList(Member{ID: "A", Addr: "A", State: StateAlive})
	drainGossip(t, l)

	if !l.Merge(Member{ID: "B", Addr: "B", Incarnation: 5, State: StateAlive}) {
		t.Fatal("Merge(B@5) = false, want true")
	}
	drainGossip(t, l)

	// Stale incarnation: no view change, so nothing may be re-queued.
	if l.Merge(Member{ID: "B", Addr: "B", Incarnation: 4, State: StateAlive}) {
		t.Fatal("Merge(B@4) = true, want false (stale incarnation)")
	}
	if got := l.PendingGossip(10, nil); len(got) != 0 {
		t.Errorf("PendingGossip after no-change Merge = %+v, want empty", got)
	}
}

func TestList_PendingGossip_LeastSpreadFirst(t *testing.T) {
	l := NewList(Member{ID: "A", Addr: "A", State: StateAlive})
	drainGossip(t, l)

	// B and C both start at the same budget; the first single-slot pull takes
	// B on the ID tie-break and lowers its remainder, so the next pull must
	// prefer C — the least-spread rumor.
	l.Merge(Member{ID: "B", Addr: "B", Incarnation: 1, State: StateAlive})
	l.Merge(Member{ID: "C", Addr: "C", Incarnation: 1, State: StateAlive})

	first := l.PendingGossip(1, nil)
	if len(first) != 1 || first[0].ID != "B" {
		t.Fatalf("first PendingGossip(1, nil) = %+v, want B (ID tie-break)", first)
	}
	second := l.PendingGossip(1, nil)
	if len(second) != 1 || second[0].ID != "C" {
		t.Errorf("second PendingGossip(1, nil) = %+v, want C (larger remaining budget)", second)
	}
}

func TestList_PendingGossip_RespectsLimit(t *testing.T) {
	l := NewList(Member{ID: "A", Addr: "A", State: StateAlive})
	drainGossip(t, l)

	for _, id := range []ID{"B", "C", "D", "E", "F"} {
		l.Merge(Member{ID: id, Addr: string(id), Incarnation: 1, State: StateAlive})
	}
	if got := l.PendingGossip(2, nil); len(got) != 2 {
		t.Errorf("PendingGossip(2, nil) returned %d records, want 2", len(got))
	}
	if got := l.PendingGossip(2, nil); len(got) != 2 {
		t.Errorf("second PendingGossip(2, nil) returned %d records, want 2", len(got))
	}
}

func TestGossipRetransmitBudget(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{1, 3},   // 3·ceil(log2(2)) = 3
		{2, 6},   // 3·ceil(log2(3)) = 6
		{7, 9},   // 3·ceil(log2(8)) = 9
		{15, 12}, // 3·ceil(log2(16)) = 12
		{31, 15}, // 3·ceil(log2(32)) = 15
	}
	for _, tt := range tests {
		if got := gossipRetransmitBudget(tt.n); got != tt.want {
			t.Errorf("gossipRetransmitBudget(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestList_MembersSorted(t *testing.T) {
	l := NewList(Member{ID: "M", Addr: "M", State: StateAlive})
	l.Merge(Member{ID: "Z", Addr: "Z", State: StateAlive})
	l.Merge(Member{ID: "A", Addr: "A", State: StateAlive})

	members := l.Members()
	gotIDs := make([]ID, 0, len(members))
	for _, m := range members {
		gotIDs = append(gotIDs, m.ID)
	}
	if want := []ID{"A", "M", "Z"}; !reflect.DeepEqual(gotIDs, want) {
		t.Errorf("Members() IDs = %v, want %v", gotIDs, want)
	}
}
