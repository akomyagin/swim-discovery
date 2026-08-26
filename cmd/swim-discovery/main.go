// Command swim-discovery runs a single SWIM node and (since Этап 5) offers a
// small CLI to observe the cluster membership as it converges.
//
// v1 is entirely localhost: launch several processes on distinct UDP ports,
// point each at a seed peer, and watch failure detection propagate.
//
//	swim-discovery run -addr 127.0.0.1:7947 [-seed 127.0.0.1:7948]
//	swim-discovery observe -addr ... [-seed ...] [-interval 1s]
//	swim-discovery -addr ...   // no subcommand: defaults to run
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/akomyagin/swim-discovery/internal/member"
	"github.com/akomyagin/swim-discovery/internal/swim"
	"github.com/akomyagin/swim-discovery/internal/transport"
)

func main() {
	// The subcommand is the first positional argument. A leading flag — or no
	// arguments at all — keeps the pre-Этап-5 invocation working unchanged:
	// `swim-discovery -addr ...` still just runs a node.
	command, args := "run", os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}
	if command != "run" && command != "observe" {
		log.Fatalf("swim-discovery: unknown command %q (available: run, observe)", command)
	}

	fs := flag.NewFlagSet(command, flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7947", "UDP address this node binds to")
	seed := fs.String("seed", "", "address of an existing node to join (empty = start a new cluster)")
	interval := time.Second
	if command == "observe" {
		fs.DurationVar(&interval, "interval", time.Second, "how often to print the membership snapshot")
	}
	fs.Parse(args) // ExitOnError: never returns on a bad flag
	if fs.NArg() > 0 {
		// flag.Parse stops at the first non-flag token and leaves the rest
		// unparsed rather than erroring — without this check a stray
		// positional (e.g. a typo'd flag missing its leading `-`) would
		// silently swallow every flag after it instead of failing loudly.
		log.Fatalf("swim-discovery: unexpected argument %q", fs.Arg(0))
	}

	runNode(*addr, *seed, command == "observe", interval)
}

// runNode assembles and runs one node; in observe mode it also starts the
// periodic membership printer. Blocks until SIGINT/SIGTERM.
func runNode(addr, seed string, observe bool, interval time.Duration) {
	// v1 convention: node ID is its "host:port" address.
	self := member.Member{ID: member.ID(addr), Addr: addr, State: member.StateAlive}
	list := member.NewList(self)

	// Pre-seeding the peer gives the probe loop its first target; the Merge
	// also queues the seed for gossip, and NewList queues self, so the join
	// becomes two-sided as soon as probes start flowing.
	if seed != "" && seed != addr {
		list.Merge(member.Member{ID: member.ID(seed), Addr: seed, State: member.StateAlive})
	}

	tr, err := transport.NewUDP(addr)
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
	node := swim.NewNode(list, tr, swim.Config{
		ProbeInterval: time.Second,
		RTTTimeout:    300 * time.Millisecond,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if observe {
		// Read-only observer: printMembers works off List.Members() — a
		// sorted snapshot copied under RLock — so the printer never blocks
		// or races the node's probe/receive loops.
		go observeLoop(ctx, list, interval)
	}

	log.Printf("swim-discovery node %s started (seed=%q)", addr, seed)
	node.Run(ctx)
	log.Printf("swim-discovery node %s stopped", addr)
}

// observeLoop periodically dumps the membership view until ctx is cancelled.
func observeLoop(ctx context.Context, list *member.List, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	start := time.Now()
	for {
		select {
		case <-ticker.C:
			printMembers(list, start)
		case <-ctx.Done():
			return
		}
	}
}

// printMembers writes one human-readable snapshot of the membership view to
// stdout (fmt, not log: a table gains nothing from per-line time prefixes).
// A Dead member stays listed for swim.Config.DeadTimeout after the death is
// declared — the observer is exactly who must see the transition — then
// disappears once the local node evicts the record (see member.List.Evict,
// Этап 6).
func printMembers(list *member.List, start time.Time) {
	members := list.Members()
	fmt.Printf("[observe %s] t=%s  members=%d\n", list.Self(), time.Since(start).Round(time.Second), len(members))
	for _, m := range members {
		selfMark := ""
		if m.ID == list.Self() {
			selfMark = "   (self)"
		}
		fmt.Printf("  %-16s %-8s inc=%d%s\n", m.Addr, m.State, m.Incarnation, selfMark)
	}
}
