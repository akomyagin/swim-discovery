// Package protocol defines the SWIM wire messages exchanged between nodes and
// their (de)serialization over transport.Packet payloads.
//
// Message families:
//   - Ping / Ack            — direct failure detection (Этап 1).
//   - PingReq / (indirect)  — indirect probing: ask K peers to ping a suspect
//     on our behalf, so losing *our* packet to the target does not by itself
//     mark it dead (Этап 3). This is SWIM's false-positive defense.
//   - Gossip updates        — piggybacked membership deltas (alive/suspect/
//     dead + incarnation) that ride along with every message (Этап 2).
//
// Encoding for v1: JSON, chosen for legibility while learning the protocol.
// A compact binary framing is a POST_MVP concern.
package protocol

import (
	"encoding/json"

	"github.com/akomyagin/swim-discovery/internal/member"
)

// Kind tags the message type in the envelope.
type Kind uint8

const (
	// KindPing — direct probe: "are you alive?".
	KindPing Kind = iota
	// KindAck — response to a Ping (or an indirect ping), proving liveness.
	KindAck
	// KindPingReq — "please ping Target on my behalf and relay the ack".
	KindPingReq
)

// Update is one gossiped membership fact piggybacked on any message.
type Update struct {
	ID          member.ID    `json:"id"`
	Addr        string       `json:"addr"`
	Incarnation uint64       `json:"incarnation"`
	State       member.State `json:"state"`
}

// Message is the envelope carried in every datagram. Every message piggybacks
// a batch of Updates so membership state propagates on ordinary probe traffic.
type Message struct {
	Kind Kind      `json:"kind"`
	From member.ID `json:"from"`
	// SeqNo correlates a Ping/PingReq with its Ack.
	SeqNo uint64 `json:"seq"`
	// Target is the node to probe; only meaningful for KindPingReq.
	Target member.ID `json:"target,omitempty"`
	// Updates are piggybacked gossip (Этап 2).
	Updates []Update `json:"updates,omitempty"`
}

// Encode serializes a Message into a transport payload.
//
// TODO(Этап 2): include piggybacked Updates.
func Encode(m Message) ([]byte, error) {
	return json.Marshal(m)
}

// Decode parses a transport payload back into a Message.
func Decode(payload []byte) (Message, error) {
	var m Message
	err := json.Unmarshal(payload, &m)
	return m, err
}
