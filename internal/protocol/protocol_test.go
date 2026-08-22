package protocol

import (
	"reflect"
	"testing"
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

func TestDecode_Garbage(t *testing.T) {
	if _, err := Decode([]byte("{ broken")); err == nil {
		t.Error("Decode of garbage returned nil error, want non-nil")
	}
}
