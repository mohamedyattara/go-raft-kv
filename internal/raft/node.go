package raft

import (
	"sync"
	"time"
	"math/rand"
	
)

type Node struct {
	// identity
    id      string
    address string
    peers   []string

    // raft state
    role        string
    currentTerm int
    votedFor    string

    // log
    log         []LogEntry
    commitIndex int
    lastApplied int

    // key-value store
    kvStore map[string]string

    // leader only
    nextIndex  map[string]int
    matchIndex map[string]int

    // coordination
    mu          sync.Mutex
    heartbeatCh chan bool
    voteCh      chan bool
    stepDownCh  chan bool

    // timing
    electionTimeout  time.Duration
    heartbeatTimeout time.Duration
}
type LogEntry struct{
	Term int
	Command string
}

func NewNode(id string, address string, peers []string) *Node {
    return &Node{
		id:               id,
        address:          address,
        peers:            peers,
        role:             "follower",
        currentTerm:      0,
        votedFor:         "",
        log:              []LogEntry{},
        commitIndex:      0,
        lastApplied:      0,
        kvStore:          make(map[string]string),
        nextIndex:        make(map[string]int),
        matchIndex:       make(map[string]int),
        heartbeatCh:      make(chan bool, 1),
        voteCh:           make(chan bool, 1),
        stepDownCh:       make(chan bool, 1),
        electionTimeout:  time.Duration(150+rand.Intn(150)) * time.Millisecond,
        heartbeatTimeout: 50 * time.Millisecond,
    }

    }


func (n *Node) Start() {
    fmt.Printf("Node %s starting on %s as %s\n", n.id, n.address, n.role)
    
    go n.runElectionTimer()
    go n.startHTTPServer()
}
