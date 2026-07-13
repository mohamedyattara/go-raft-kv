package chaos_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

var (
	node1Cmd *exec.Cmd
	node2Cmd *exec.Cmd
	node3Cmd *exec.Cmd
)

var nodes = []string{
	"localhost:8001",
	"localhost:8002",
	"localhost:8003",
}

func TestMain(m *testing.M) {
	var err error

	node1Cmd, err = startNode("node-1", "localhost:8001", "localhost:8002,localhost:8003")
	if err != nil {
		fmt.Printf("failed to start node-1: %v\n", err)
		os.Exit(1)
	}

	node2Cmd, err = startNode("node-2", "localhost:8002", "localhost:8001,localhost:8003")
	if err != nil {
		fmt.Printf("failed to start node-2: %v\n", err)
		os.Exit(1)
	}

	node3Cmd, err = startNode("node-3", "localhost:8003", "localhost:8001,localhost:8002")
	if err != nil {
		fmt.Printf("failed to start node-3: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Waiting for cluster to stabilize...")
	time.Sleep(10 * time.Second)

	code := m.Run()

	killNode(node1Cmd)
	killNode(node2Cmd)
	killNode(node3Cmd)

	os.Exit(code)
}

func startNode(id, address, peers string) (*exec.Cmd, error) {
	cmd := exec.Command(
		"go", "run", "./cmd/node/main.go",
		"--id="+id,
		"--address="+address,
		"--peers="+peers,
	)
	cmd.Dir = "../"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start %s: %w", id, err)
	}

	return cmd, nil
}

func killNode(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
	time.Sleep(2 * time.Second)
}

func sendToLeader(command string) (map[string]string, error) {
	for _, node := range nodes {
		result, err := sendRequest(node, command)
		if err != nil {
			continue
		}
		if result["error"] == "not_leader" {
			continue
		}
		return result, nil
	}
	return nil, fmt.Errorf("no leader found")
}

func sendRequest(target, command string) (map[string]string, error) {
	body, err := json.Marshal(map[string]string{
		"command": command,
	})
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(
		"http://"+target+"/client-request",
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"node %s returned status %d",
			target,
			resp.StatusCode,
		)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func getNodeStatus(target string) (map[string]interface{}, error) {
	resp, err := http.Get("http://" + target + "/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func TestBasicReplication(t *testing.T) {
	t.Log("Starting TestBasicReplication...")

	result, err := sendToLeader("PUT name Mohamed")
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	if result["status"] != "ok" {
		t.Fatalf("PUT failed: %+v", result)
	}
	fmt.Println("PUT succeeded")

	time.Sleep(1 * time.Second)

	for _, addr := range nodes {
		status, err := getNodeStatus(addr)
		if err != nil {
			t.Errorf("failed to get status from %s: %v", addr, err)
			continue
		}
		kvStore, ok := status["kv_store"].(map[string]interface{})
		if !ok {
			t.Errorf("node %s has no kvStore", addr)
			continue
		}
		if kvStore["name"] != "Mohamed" {
			t.Errorf("node %s expected Mohamed, got %v", addr, kvStore["name"])
		} else {
			fmt.Printf("node %s has correct value: Mohamed \n", addr)
		}
	}
}

func TestLeaderFailure(t *testing.T) {
	t.Log("Starting TestLeaderFailure...")

	result, err := sendToLeader("PUT city Columbus")
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	if result["status"] != "ok" {
		t.Fatalf("PUT failed: %+v", result)
	}
	fmt.Println("PUT succeeded")

	// find the actual leader
	leaderAddr := ""
	for _, addr := range nodes {
		status, err := getNodeStatus(addr)
		if err != nil {
			continue
		}
		if status["role"] == "leader" {
			leaderAddr = addr
			break
		}
	}

	if leaderAddr == "" {
		t.Fatal("could not identify leader")
	}

	fmt.Printf("Killing leader at %s...\n", leaderAddr)
	switch leaderAddr {
	case "localhost:8001":
		killNode(node1Cmd)
		node1Cmd = nil
	case "localhost:8002":
		killNode(node2Cmd)
		node2Cmd = nil
	case "localhost:8003":
		killNode(node3Cmd)
		node3Cmd = nil
	}

	t.Log("Waiting for re-election...")
	time.Sleep(10 * time.Second)

	result, err = sendToLeader("GET city")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if result["value"] != "Columbus" {
		t.Fatalf("expected Columbus, got %+v", result)
	} else {
		fmt.Println("Data survived leader failure: Columbus ")
	}
}
