// Package swim implements the SWIM protocol cycles. Этап 1 covers the direct
// probe loop: every ProbeInterval pick one random peer, send a Ping, and
// confirm it alive when the matching Ack arrives. Indirect probing (Этап 3)
// and suspicion timeouts (Этап 4) build on top of this loop.
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

// Config holds probe-loop timing and randomness. Time stays system-driven in
// Этап 1 (context deadlines only); a Clock abstraction arrives in Этап 4.
type Config struct {
	ProbeInterval time.Duration // how often to probe one random peer
	RTTTimeout    time.Duration // how long to wait for an Ack
	// Rand is the injectable RNG for peer selection: tests seed it so the
	// probed target is deterministic instead of global-rand luck.
	Rand *rand.Rand
}

// Node runs the SWIM cycles for one cluster member over a Transport.
type Node struct {
	list *member.List
	tr   transport.Transport
	cfg  Config

	mu    sync.Mutex
	seqNo uint64 // monotonically increasing; correlates Ping with Ack

	// pending correlates an outstanding Ping's SeqNo with a channel the
	// receive loop signals when the matching Ack arrives.
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
	return &Node{
		list:    list,
		tr:      tr,
		cfg:     cfg,
		pending: map[uint64]chan struct{}{},
	}
}

// Run starts the receive and probe loops and blocks until ctx is cancelled.
func (n *Node) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		n.receiveLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		n.probeLoop(ctx)
	}()
	wg.Wait()
}

func (n *Node) receiveLoop(ctx context.Context) {
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
		switch msg.Kind {
		case protocol.KindPing:
			// Echo SeqNo so the sender can correlate. Updates stay empty
			// until gossip lands in Этап 2.
			ack := protocol.Message{
				Kind:  protocol.KindAck,
				From:  n.list.Self(),
				SeqNo: msg.SeqNo,
			}
			payload, err := protocol.Encode(ack)
			if err != nil {
				log.Printf("swim: encode ack: %v", err)
				continue
			}
			if err := n.tr.Send(ctx, pkt.Addr, payload); err != nil {
				log.Printf("swim: send ack to %s: %v", pkt.Addr, err)
			}
		case protocol.KindAck:
			n.mu.Lock()
			ch, ok := n.pending[msg.SeqNo]
			if ok {
				delete(n.pending, msg.SeqNo)
			}
			n.mu.Unlock()
			if ok {
				// Buffered channel: the send never blocks and never panics,
				// even if probeOnce already gave up on this seq.
				ch <- struct{}{}
			}
		case protocol.KindPingReq:
			// Indirect probing is Этап 3; ignoring is the whole handling.
			log.Printf("swim: PingReq from %s ignored (not implemented until Этап 3)", msg.From)
		}
	}
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

// probeOnce probes a single random peer and waits up to RTTTimeout for its
// Ack. On Ack the peer is (re)confirmed alive; on timeout Этап 1 deliberately
// does nothing — no Suspect transition (Этап 4), no PingReq (Этап 3).
func (n *Node) probeOnce(ctx context.Context) {
	others := n.list.Others()
	if len(others) == 0 {
		return // single-node cluster: nobody to probe
	}

	// cfg.Rand (*rand.Rand) is not safe for concurrent use; share n.mu with
	// seqNo/pending rather than add a second lock, since Этап 3 will also
	// draw K mediators from the same Rand and may call this concurrently.
	n.mu.Lock()
	target := others[n.cfg.Rand.Intn(len(others))]
	n.seqNo++
	seq := n.seqNo
	// Buffer of 1 lets receiveLoop signal without close and without blocking.
	ackCh := make(chan struct{}, 1)
	n.pending[seq] = ackCh
	n.mu.Unlock()

	msg := protocol.Message{
		Kind:  protocol.KindPing,
		From:  n.list.Self(),
		SeqNo: seq,
	}
	payload, err := protocol.Encode(msg)
	if err != nil {
		log.Printf("swim: encode ping: %v", err)
	} else if err := n.tr.Send(ctx, target.Addr, payload); err != nil {
		log.Printf("swim: send ping to %s: %v", target.Addr, err)
	}

	pctx, cancel := context.WithTimeout(ctx, n.cfg.RTTTimeout)
	defer cancel()

	acked := false
	select {
	case <-ackCh:
		acked = true
	case <-pctx.Done():
	}

	// Idempotent cleanup: whoever did not consume the entry removes it.
	n.mu.Lock()
	delete(n.pending, seq)
	n.mu.Unlock()

	if acked {
		// Re-assert liveness at the known incarnation; Merge returning false
		// is expected when nothing actually changed.
		n.list.Merge(member.Member{
			ID:          target.ID,
			Addr:        target.Addr,
			Incarnation: target.Incarnation,
			State:       member.StateAlive,
		})
	} else {
		log.Printf("swim: no ack from %s (seq=%d) — indirect probe deferred to Этап 3", target.ID, seq)
	}
}
