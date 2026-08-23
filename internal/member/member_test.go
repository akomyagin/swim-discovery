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
