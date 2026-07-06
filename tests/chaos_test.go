package chaos_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"testing"
	"time"
)

func startNode(id string, address string, peers string) *exec.Cmd {
	cmd := exec.Command("go", "run", "/cmd/node/main.go",
		"--id="+id,
		"--address="+address,
		"--peers="+peers,
	)
	cmd.Start()
	return cmd
}

func killNode(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		cmd.Process.Kill()
	}
}
func sendToLeader(command string) (map[string]string, error) {
	nodes := []string{"localhost:8001", "localhost:8002", "localhost:8003"}

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

func sendRequest(target string, command string) (map[string]string, error) {
	body, _ := json.Marshal(map[string]string{
		"command": command,
	})

	resp, err := http.Post("http://"+target+"/client-request", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}
func TestBasicReplication(t *testing.T) {
	fmt.Println("Starting TestBasicReplication...")

	node1 := startNode("node-1", "localhost:8001", "localhost:8002,localhost:8003")
	node2 := startNode("node-2", "localhost:8002", "localhost:8001,localhost:8003")
	node3 := startNode("node-3", "localhost:8003", "localhost:8001,localhost:8002")

	defer killNode(node1)
	defer killNode(node2)
	defer killNode(node3)

	fmt.Println("Waiting for leader election...")
	time.Sleep(10 * time.Second)

	result, err := sendToLeader("PUT name Mohamed")
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	if result["status"] != "ok" {
		t.Fatalf("PUT failed: %v", result)
	}
	fmt.Println("PUT succeeded")

	time.Sleep(1 * time.Second)

	for _, addr := range []string{"localhost:8001", "localhost:8002", "localhost:8003"} {
		result, err := sendRequest(addr, "GET name")
		if err != nil {
			t.Errorf("GET from %s failed: %v", addr, err)
			continue
		}
		if result["value"] != "Mohamed" {
			t.Errorf("Node %s has wrong value: %v", addr, result)
		} else {
			fmt.Printf("Node %s correctly has value: Mohamed\n", addr)
		}
	}
}
func TestLeaderFailure(t *testing.T) {
	fmt.Println("Starting TestLeaderFailure...")

	node1 := startNode("node-1", "localhost:8001", "localhost:8002,localhost:8003")
	node2 := startNode("node-2", "localhost:8002", "localhost:8001,localhost:8003")
	node3 := startNode("node-3", "localhost:8003", "localhost:8001,localhost:8002")

	defer killNode(node2)
	defer killNode(node3)

	fmt.Println("Waiting for leader election...")
	time.Sleep(5 * time.Second)

	result, err := sendToLeader("PUT city Columbus")
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	if result["status"] != "ok" {
		t.Fatalf("PUT failed: %v", result)
	}
	fmt.Println("PUT succeeded, now killing node-1...")

	killNode(node1)
	fmt.Println("Node-1 killed, waiting for re-election...")
	time.Sleep(10 * time.Second)

	result, err = sendToLeader("GET city")
	if err != nil {
		t.Fatalf("GET failed after leader failure: %v", err)
	}
	if result["value"] != "Columbus" {
		t.Errorf("Data lost after leader failure: %v", result)
	} else {
		fmt.Println("Data survived leader failure: Columbus ")
	}
}
