package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/mohamedyattara/go-raft-kv/internal/raft"
)

func main() {
	id := flag.String("id", "node-1", "node id")
	address := flag.String("address", "localhost:8001", "node address")
	peersFlag := flag.String("peers", "", "comma separated list of peer addresses")
	flag.Parse()

	peers := []string{}
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	fmt.Printf("Starting node %s on %s\n", *id, *address)

	node := raft.NewNode(*id, *address, peers)
	node.Start()

	select {}
}