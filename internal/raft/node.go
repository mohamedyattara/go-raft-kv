package raft

import (
	"sync"
	"time"
	"math/rand"
	"fmt"
	"net/http"
    "encoding/json"
     "strings"
     "bytes"
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
func (n *Node) startHTTPServer() {
    mux := http.NewServeMux()
    mux.HandleFunc("/request-vote", n.handleRequestVote)
    mux.HandleFunc("/append-entries", n.handleAppendEntries)
    mux.HandleFunc("/client-request", n.handleClientRequest)

    server := &http.Server{
        Addr:    n.address,
        Handler: mux,
    }

    fmt.Printf("Node %s listening on %s\n", n.id, n.address)
    if err := server.ListenAndServe(); err != nil {
        fmt.Printf("Node %s HTTP server error: %s\n", n.id, err)
    }
}
type VoteRequest struct {
    Term        int    `json:"term"`
    CandidateID string `json:"candidate_id"`
}

type VoteResponse struct {
    Term        int  `json:"term"`
    VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesRequest struct {
    Term     int        `json:"term"`
    LeaderID string     `json:"leader_id"`
    Entries  []LogEntry `json:"entries"`
    LeaderCommit int   `json:"leader_commit"`
}

type AppendEntriesResponse struct {
    Term    int  `json:"term"`
    Success bool `json:"success"`
}
func (n *Node) handleRequestVote(w http.ResponseWriter, r *http.Request) {
    var req VoteRequest
    json.NewDecoder(r.Body).Decode(&req)

    n.mu.Lock()
    defer n.mu.Unlock()

    resp := VoteResponse{Term: n.currentTerm, VoteGranted: false}

    if req.Term > n.currentTerm {
        n.currentTerm = req.Term
        n.role = "follower"
        n.votedFor = ""
    }

    if req.Term >= n.currentTerm && (n.votedFor == "" || n.votedFor == req.CandidateID) {
        n.votedFor = req.CandidateID
        resp.VoteGranted = true
    }

    json.NewEncoder(w).Encode(resp)
}
func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	json.NewDecoder(r.Body).Decode(&req)

	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{Term: n.currentTerm, Success: false}

	if req.Term < n.currentTerm {
		json.NewEncoder(w).Encode(resp)
		return
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
	}

	n.role = "follower"

	select {
	case n.heartbeatCh <- true:
	default:
	}

	if len(req.Entries) > 0 {
		n.log = append(n.log, req.Entries...)
	}

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = req.LeaderCommit
		n.applyEntries()
	}

	resp.Success = true
	json.NewEncoder(w).Encode(resp)
}
func (n *Node) handleClientRequest(w http.ResponseWriter, r *http.Request) {
	var req map[string]string
	json.NewDecoder(r.Body).Decode(&req)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != "leader" {
		json.NewEncoder(w).Encode(map[string]string{
			"error":  "not_leader",
			"leader": n.votedFor,
		})
		return
	}

	command := req["command"]
	entry := LogEntry{
		Term:    n.currentTerm,
		Command: command,
	}
	n.log = append(n.log, entry)

	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}
func (n *Node) applyEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		parts := strings.Split(entry.Command, " ")

		if len(parts) == 3 && parts[0] == "PUT" {
			n.kvStore[parts[1]] = parts[2]
		}
	}
}
func (n *Node) runElectionTimer() {
	for {
		timeout := time.NewTimer(n.electionTimeout)

		select {
		case <-n.heartbeatCh:
			timeout.Stop()
		case <-timeout.C:
			n.mu.Lock()
			if n.role != "leader" {
				n.mu.Unlock()
				n.startElection()
			} else {
				n.mu.Unlock()
			}
		}
	}
}
func (n *Node) startElection() {
	n.mu.Lock()
	n.role = "candidate"
	n.currentTerm++
	n.votedFor = n.id
	votes := 1
	term := n.currentTerm
	n.mu.Unlock()

	fmt.Printf("Node %s starting election for term %d\n", n.id, term)

	for _, peer := range n.peers {
		go func(peer string) {
			req := VoteRequest{
				Term:        term,
				CandidateID: n.id,
			}
			resp, err := n.sendVoteRequest(peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.role = "follower"
				n.votedFor = ""
				return
			}
			if resp.VoteGranted {
				votes++
				if votes > len(n.peers)/2 {
					n.role = "leader"
					fmt.Printf("Node %s became leader for term %d\n", n.id, term)
					go n.runHeartbeatSender()
				}
			}
		}(peer)
	}
}
func (n *Node) sendVoteRequest(peer string, req VoteRequest) (VoteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return VoteResponse{}, err
	}

	resp, err := http.Post("http://"+peer+"/request-vote", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return VoteResponse{}, err
	}
	defer resp.Body.Close()

	var voteResp VoteResponse
	json.NewDecoder(resp.Body).Decode(&voteResp)
	return voteResp, nil
}
func (n *Node) runHeartbeatSender() {
	for {
		n.mu.Lock()
		if n.role != "leader" {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		for _, peer := range n.peers {
			go func(peer string) {
				req := AppendEntriesRequest{
					Term:         n.currentTerm,
					LeaderID:     n.id,
					Entries:      []LogEntry{},
					LeaderCommit: n.commitIndex,
				}
				n.sendAppendEntries(peer, req)
			}(peer)
		}

		time.Sleep(n.heartbeatTimeout)
	}
}
func (n *Node) sendAppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	resp, err := http.Post("http://"+peer+"/append-entries", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	defer resp.Body.Close()

	var appendResp AppendEntriesResponse
	json.NewDecoder(resp.Body).Decode(&appendResp)
	return appendResp, nil
}