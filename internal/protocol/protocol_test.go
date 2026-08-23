package protocol

import (
	"reflect"
	"testing"

	"github.com/akomyagin/swim-discovery/internal/member"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
	}{
		{
			name: "ping",
			msg:  Message{Kind: KindPing, From: "127.0.0.1:7947", SeqNo: 42},
		},
		{
			name: "ack",
			msg:  Message{Kind: KindAck, From: "127.0.0.1:7948", SeqNo: 42},
		},
		{
			// The envelope must round-trip Target already; PingReq handling
			// itself is Этап 3.
			name: "pingreq with target",
			msg:  Message{Kind: KindPingReq, From: "127.0.0.1:7947", SeqNo: 7, Target: "127.0.0.1:7949"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := Encode(tt.msg)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := Decode(payload)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tt.msg) {
				t.Errorf("round-trip = %+v, want %+v", got, tt.msg)
			}
			if len(got.Updates) != 0 {
				t.Errorf("Updates after round-trip = %+v, want empty (omitempty)", got.Updates)
			}
		})
	}
}

// TestEncodeDecode_WithUpdates fixes that a piggybacked gossip batch survives
// the wire round-trip with every field (including State) intact.
func TestEncodeDecode_WithUpdates(t *testing.T) {
	msg := Message{
		Kind:  KindPing,
		From:  "A",
		SeqNo: 1,
		Updates: []Update{
			{ID: "B", Addr: "B", Incarnation: 2, State: member.StateSuspect},
			{ID: "C", Addr: "C", Incarnation: 5, State: member.StateAlive},
		},
	}
	payload, err := Encode(msg)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Updates) != 2 {
		t.Fatalf("Updates after round-trip = %d records, want 2", len(got.Updates))
	}
	if !reflect.DeepEqual(got, msg) {
		t.Errorf("round-trip = %+v, want %+v", got, msg)
	}
}

func TestDecode_Garbage(t *testing.T) {
	if _, err := Decode([]byte("{ broken")); err == nil {
		t.Error("Decode of garbage returned nil error, want non-nil")
	}
}
