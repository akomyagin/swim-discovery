// Command swim-discovery runs a single SWIM node and (Этап 5) offers a small
// CLI to observe the cluster membership as it converges.
//
// v1 is entirely localhost: launch several processes on distinct UDP ports,
// point each at a seed peer, and watch failure detection propagate.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	var (
		addr = flag.String("addr", "127.0.0.1:7947", "UDP address this node binds to")
		seed = flag.String("seed", "", "address of an existing node to join (empty = start a new cluster)")
	)
	flag.Parse()

	// TODO(Этап 1): bind transport.NewUDP(*addr), build member.List, run the
	// direct ping/ack probe loop.
	// TODO(Этап 2): start the gossip dissemination loop.
	// TODO(Этап 3): wire indirect probing (PingReq to K random peers).
	// TODO(Этап 4): run the suspicion-timeout scheduler.
	// TODO(Этап 5): add the `members`/observe CLI subcommand.
	fmt.Fprintf(os.Stderr, "swim-discovery: not implemented yet (addr=%s seed=%q)\n", *addr, *seed)
	os.Exit(1)
}
