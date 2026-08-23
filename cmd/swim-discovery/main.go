// Command swim-discovery runs a single SWIM node and (Этап 5) offers a small
// CLI to observe the cluster membership as it converges.
//
// v1 is entirely localhost: launch several processes on distinct UDP ports,
// point each at a seed peer, and watch failure detection propagate.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akomyagin/swim-discovery/internal/member"
	"github.com/akomyagin/swim-discovery/internal/swim"
	"github.com/akomyagin/swim-discovery/internal/transport"
)

func main() {
	var (
		addr = flag.String("addr", "127.0.0.1:7947", "UDP address this node binds to")
		seed = flag.String("seed", "", "address of an existing node to join (empty = start a new cluster)")
	)
	flag.Parse()

	// v1 convention: node ID is its "host:port" address.
	self := member.Member{ID: member.ID(*addr), Addr: *addr, State: member.StateAlive}
	list := member.NewList(self)

	// Pre-seeding the peer gives the probe loop its first target; the Merge
	// also queues the seed for gossip, and NewList queues self, so the join
	// becomes two-sided as soon as probes start flowing.
	if *seed != "" && *seed != *addr {
		list.Merge(member.Member{ID: member.ID(*seed), Addr: *seed, State: member.StateAlive})
	}

	tr, err := transport.NewUDP(*addr)
	if err != nil {
		log.Fatalf("swim-discovery: %v", err)
	}
	defer tr.Close()

	// Gossip needs no dedicated loop: membership updates piggyback on the
	// probe/ack traffic that Node.Run already drives (Этап 2). Indirect
	// probing (Этап 3) and suspicion timeouts with refute (Этап 4) also need
	// no wiring here: NewNode defaults IndirectNodes/IndirectTimeout, the
	// production Clock and SuspicionTimeout, and Node.Run arms/disarms the
	// suspicion timers on its own.
	// TODO(Этап 5): add the `members`/observe CLI subcommand.
	node := swim.NewNode(list, tr, swim.Config{
		ProbeInterval: time.Second,
		RTTTimeout:    300 * time.Millisecond,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("swim-discovery node %s started (seed=%q)", *addr, *seed)
	node.Run(ctx)
	log.Printf("swim-discovery node %s stopped", *addr)
}
