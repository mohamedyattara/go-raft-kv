package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
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
type LogEntry struct {
	Term    int
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
		commitIndex:      -1,
		lastApplied:      -1,
		kvStore:          make(map[string]string),
		nextIndex:        make(map[string]int),
		matchIndex:       make(map[string]int),
		heartbeatCh:      make(chan bool, 1),
		voteCh:           make(chan bool, 1),
		stepDownCh:       make(chan bool, 1),
		electionTimeout:  time.Duration(3000+rand.Intn(1500)) * time.Millisecond,
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
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
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
		select {
		case n.heartbeatCh <- true:
		default:
		}
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
	//fmt.Printf("Node %s received heartbeat from %s term %d\n", n.id, req.LeaderID, req.Term)
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
		fmt.Printf("Node %s applied entries up to index %d\n", n.id, n.commitIndex)
	}

	resp.Success = true
	json.NewEncoder(w).Encode(resp)
}
func (n *Node) handleClientRequest(w http.ResponseWriter, r *http.Request) {
	var req map[string]string
	json.NewDecoder(r.Body).Decode(&req)

	n.mu.Lock()

	if n.role != "leader" {
		n.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{
			"error":  "not_leader",
			"leader": n.votedFor,
		})
		return
	}

	command := req["command"]
	parts := strings.Split(command, " ")
	if parts[0] == "GET" && len(parts) == 2 {
		value, ok := n.kvStore[parts[1]]
		n.mu.Unlock()
		if !ok {
			json.NewEncoder(w).Encode(map[string]string{
				"error": "key not found",
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{
			"value": value,
		})
		return
	}
	entry := LogEntry{
		Term:    n.currentTerm,
		Command: command,
	}
	n.log = append(n.log, entry)
	logIndex := len(n.log) - 1
	term := n.currentTerm
	n.mu.Unlock()

	confirmed := 1
	var mu sync.Mutex
	confirmCh := make(chan bool, len(n.peers))

	for _, peer := range n.peers {
		go func(peer string) {
			n.mu.Lock()
			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.id,
				Entries:      []LogEntry{entry},
				LeaderCommit: n.commitIndex,
			}
			n.mu.Unlock()
			resp, err := n.sendAppendEntries(peer, req)
			if err != nil {
				confirmCh <- false
				return
			}
			if resp.Success {
				n.mu.Lock()
				n.matchIndex[peer] = logIndex
				n.nextIndex[peer] = logIndex + 1
				n.mu.Unlock()
				confirmCh <- true
			} else {
				confirmCh <- false
			}
		}(peer)
	}

	for i := 0; i < len(n.peers); i++ {
		if <-confirmCh {
			mu.Lock()
			confirmed++
			mu.Unlock()
		}
	}
	mu.Lock()
	totalNodes := len(n.peers) + 1
	majority := totalNodes/2 + 1
	currentConfirmed := confirmed
	mu.Unlock()

	if currentConfirmed >= majority {
		n.mu.Lock()
		if logIndex > n.commitIndex {
			n.commitIndex = logIndex
			n.applyEntries()
		}
		n.mu.Unlock()

		fmt.Printf("Node %s committed entry %d: %s\n", n.id, logIndex, command)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
		})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{
		"error": "failed to replicate to majority",
	})
}

func (n *Node) applyEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
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

	if len(n.peers) == 0 {
		n.mu.Lock()
		n.role = "leader"
		n.mu.Unlock()
		fmt.Printf("Node %s became leader for term %d (single node)\n", n.id, term)
		go n.runHeartbeatSender()
		return
	}
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
				var totalNodes int = len(n.peers) + 1
				if votes > totalNodes/2 && n.role != "leader" {
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
