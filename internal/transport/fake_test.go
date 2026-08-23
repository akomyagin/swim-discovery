package transport

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestFake_SendReceive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()

	payload := []byte("ping")
	if err := a.Send(ctx, "B", payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	pkt, err := b.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if !bytes.Equal(pkt.Payload, payload) {
		t.Errorf("payload = %q, want %q", pkt.Payload, payload)
	}
	if pkt.Addr != "A" {
		t.Errorf("pkt.Addr = %q, want sender address %q", pkt.Addr, "A")
	}
}

func TestFake_SendUnknownAddr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()

	// Like UDP to a dark address: best-effort, no error, no panic.
	if err := a.Send(ctx, "Z", []byte("into the void")); err != nil {
		t.Errorf("Send to unknown addr = %v, want nil", err)
	}
}

func TestFake_Close(t *testing.T) {
	network := NewFakeNetwork()
	a := network.Endpoint("A")
	b := network.Endpoint("B")
	defer b.Close()

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Errorf("second Close = %v, want nil (idempotent)", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Sending to a closed endpoint must neither error nor panic.
	if err := b.Send(ctx, "A", []byte("late")); err != nil {
		t.Errorf("Send to closed endpoint = %v, want nil", err)
	}

	// Receive on the closed endpoint returns via ctx, not a stuck goroutine.
	rctx, rcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer rcancel()
	if _, err := a.Receive(rctx); err == nil {
		t.Error("Receive on closed endpoint = nil error, want ctx deadline error")
	}
}

func TestFake_PayloadCopied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	network := NewFakeNetwork()
	a := network.Endpoint("A")
	defer a.Close()
	b := network.Endpoint("B")
	defer b.Close()

	buf := []byte("original")
	if err := a.Send(ctx, "B", buf); err != nil {
		t.Fatalf("Send: %v", err)
	}
	copy(buf, "mutated!")

	pkt, err := b.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if !bytes.Equal(pkt.Payload, []byte("original")) {
		t.Errorf("payload = %q, want %q (sender buffer must be copied)", pkt.Payload, "original")
	}
}
