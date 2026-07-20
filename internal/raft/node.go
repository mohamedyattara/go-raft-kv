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
	id       string
	address  string
	peers    []string
	leaderID string

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
type NodeStatus struct {
	ID          string            `json:"id"`
	Role        string            `json:"role"`
	Term        int               `json:"term"`
	CommitIndex int               `json:"commit_index"`
	LastApplied int               `json:"last_applied"`
	LogLength   int               `json:"log_length"`
	KVStore     map[string]string `json:"kv_store"`
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
	mux.HandleFunc("/status", n.handleStatus)
	mux.HandleFunc("/", n.handleDashboard)

	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})
	server := &http.Server{
		Addr:    n.address,
		Handler: corsHandler,
	}

	fmt.Printf("Node %s listening on %s\n", n.id, n.address)
	if err := server.ListenAndServe(); err != nil {
		fmt.Printf("Node %s HTTP server error: %s\n", n.id, err)
	}
}

type VoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
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
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
}

type AppendEntriesResponse struct {
	Term         int  `json:"term"`
	Success      bool `json:"success"`
	LastLogIndex int  `json:"last_log_index"`
}

func (n *Node) handleDashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "dashboard.html")
}

func (n *Node) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	resp := VoteResponse{Term: n.currentTerm, VoteGranted: false}

	if req.Term < n.currentTerm {
		json.NewEncoder(w).Encode(resp)
		return
	}
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = "follower"
		n.votedFor = ""
	}
	resp.Term = n.currentTerm

	myLastIndex, myLastTerm := n.lastLogIndexAndTerm() // however you expose this
	candidateUpToDate := req.LastLogTerm > myLastTerm ||
		(req.LastLogTerm == myLastTerm && req.LastLogIndex >= myLastIndex)

	if (n.votedFor == "" || n.votedFor == req.CandidateID) && candidateUpToDate {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		select {
		case n.heartbeatCh <- true:
		default:
		}
	}

	json.NewEncoder(w).Encode(resp)
}
func (n *Node) lastLogIndexAndTerm() (int, int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	lastIdx := len(n.log) - 1
	return lastIdx, n.log[lastIdx].Term
}

func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	resp := AppendEntriesResponse{Term: n.currentTerm, Success: false, LastLogIndex: len(n.log) - 1}
	if req.Term < n.currentTerm {
		json.NewEncoder(w).Encode(resp)
		return
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""

	}

	n.role = "follower"
	n.leaderID = req.LeaderID
	//fmt.Printf("Node %s received heartbeat from %s term %d\n", n.id, req.LeaderID, req.Term)
	select {
	case n.heartbeatCh <- true:
	default:
	}
	resp.Term = n.currentTerm

	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= len(n.log) {
			// we don't even have an entry at PrevLogIndex
			resp.LastLogIndex = len(n.log) - 1
			json.NewEncoder(w).Encode(resp)
			return
		}
		if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			// entry exists but term mismatch -- conflict
			resp.LastLogIndex = len(n.log) - 1
			json.NewEncoder(w).Encode(resp)
			return
		}
	}
	if len(req.Entries) > 0 {
		insertAt := req.PrevLogIndex + 1
		for i, entry := range req.Entries {
			idx := insertAt + i
			if idx < len(n.log) {
				if n.log[idx].Term != entry.Term {
					// conflict: truncate here and start appending fresh
					n.log = n.log[:idx]
					n.log = append(n.log, req.Entries[i:]...)
					break
				}
				// else: identical entry already present, skip
			} else {
				n.log = append(n.log, req.Entries[i:]...)
				break
			}
		}
	}

	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := len(n.log) - 1
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.applyEntries()
		fmt.Printf("Node %s applied entries up to index %d\n", n.id, n.commitIndex)
	}

	resp.Success = true
	resp.LastLogIndex = len(n.log) - 1
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
			"leader": n.leaderID,
		})
		return
	}

	command := req["command"]
	parts := strings.Split(command, " ")

	// handle GET directly
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

	// append to leader log
	entry := LogEntry{
		Term:    n.currentTerm,
		Command: command,
	}
	n.log = append(n.log, entry)
	logIndex := len(n.log) - 1
	n.mu.Unlock()

	// wait for this entry to be committed
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		if n.commitIndex >= logIndex {
			n.mu.Unlock()
			fmt.Printf("Node %s committed entry %d: %s\n", n.id, logIndex, command)
			json.NewEncoder(w).Encode(map[string]string{
				"status": "ok",
			})
			return
		}
		n.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	json.NewEncoder(w).Encode(map[string]string{
		"error": "failed to commit entry",
	})
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	n.mu.Lock()
	defer n.mu.Unlock()

	status := NodeStatus{
		ID:          n.id,
		Role:        n.role,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LogLength:   len(n.log),
		LastApplied: n.lastApplied,
		KVStore:     n.kvStore,
	}

	json.NewEncoder(w).Encode(status)
}

func (n *Node) applyEntries() {
	for n.lastApplied < n.commitIndex {
		tentry := n.lastApplied + 1
		if tentry >= len(n.log) {
			break
		}
		n.lastApplied = tentry
		entry := n.log[n.lastApplied]
		parts := strings.Split(entry.Command, " ")

		if len(parts) == 3 && parts[0] == "PUT" {
			n.kvStore[parts[1]] = parts[2]
		}
	}
}
func (n *Node) resetElectionTimeout() {
	n.electionTimeout =
		time.Duration(3000+rand.Intn(1500)) * time.Millisecond
}
func (n *Node) runElectionTimer() {
	for {
		n.resetElectionTimeout()
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
	lastIndex, lastTerm := n.lastLogIndexAndTerm()
	n.leaderID = ""
	n.currentTerm++
	n.votedFor = n.id
	votes := 1
	term := n.currentTerm
	n.mu.Unlock()

	fmt.Printf("Node %s starting election for term %d\n", n.id, term)

	if len(n.peers) == 0 {
		n.mu.Lock()
		n.role = "leader"
		n.resetLeaderState()
		n.mu.Unlock()
		fmt.Printf("Node %s became leader for term %d (single node)\n", n.id, term)
		go n.runHeartbeatSender()
		return
	}
	for _, peer := range n.peers {
		go func(peer string) {
			req := VoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			resp, err := n.sendVoteRequest(peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if term != n.currentTerm || n.role != "candidate" {
				return
			}

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
					n.resetLeaderState()
					go n.runHeartbeatSender()
				}
			}
		}(peer)
	}
}
func (n *Node) resetLeaderState() {
	for _, peer := range n.peers {
		n.nextIndex[peer] = len(n.log)
		n.matchIndex[peer] = -1
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

		for _, peer := range n.peers {
			nextIdx := n.nextIndex[peer]
			entries := []LogEntry{}
			if nextIdx < len(n.log) {
				entries = n.log[nextIdx:]
			}

			prevLogIndex := nextIdx - 1
			prevLogTerm := 0
			if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
				prevLogTerm = n.log[prevLogIndex].Term
			}
			req := AppendEntriesRequest{
				Term:         n.currentTerm,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}

			go func(peer string, req AppendEntriesRequest, nextIdx int) {
				resp, err := n.sendAppendEntries(peer, req)
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

				if resp.Success {
					if len(req.Entries) > 0 {
						n.nextIndex[peer] = nextIdx + len(req.Entries)
						n.matchIndex[peer] = n.nextIndex[peer] - 1
					}
					n.tryCommit()
				} else {
					n.nextIndex[peer] = resp.LastLogIndex + 1
					if n.nextIndex[peer] < 0 {
						n.nextIndex[peer] = 0
					}
				}
			}(peer, req, nextIdx)
		}

		n.mu.Unlock()
		time.Sleep(n.heartbeatTimeout)
	}
}
func (n *Node) tryCommit() {
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}

		replicated := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicated++
			}
		}

		totalNodes := len(n.peers) + 1
		if replicated > totalNodes/2 {
			n.commitIndex = idx
			n.applyEntries()
			fmt.Printf("Node %s committed index %d\n", n.id, idx)
			break
		}
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
