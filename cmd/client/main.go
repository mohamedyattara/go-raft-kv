package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
)

func main() {
	target := flag.String("target", "localhost:8001", "node address to send request to")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: client --target=localhost:8001 PUT key value")
		fmt.Println("       client --target=localhost:8001 GET key")
		os.Exit(1)
	}

	command := strings.Join(args, " ")

	body, _ := json.Marshal(map[string]string{
		"command": command,
	})

	resp, err := http.Post("http://"+*target+"/client-request", "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("Error sending request: %s\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	fmt.Println(result)
}
