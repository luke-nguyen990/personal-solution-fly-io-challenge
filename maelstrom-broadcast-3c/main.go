package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var MAX_REFIRE int = 2
var RETRY_INTERVAL int = 1000 // in milliseconds

func main() {
	n := maelstrom.NewNode()
	values := make(map[float64]bool)
	var mutex = sync.RWMutex{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		value := body["message"].(float64)

		// Add mutex to avoid race condition
		mutex.Lock()
		if _, existed := values[value]; existed {
			return nil
		}
		values[value] = true
		mutex.Unlock()

		// Run goroutines to broadcast asynchronously with multiple requests
		for threadCounter := 0; threadCounter < MAX_REFIRE; threadCounter++ {
			go func() {
				// Broadcast value to all nodes in the network
				for _, value := range n.NodeIDs() {
					n.RPC(value, msg.Body, func(msg maelstrom.Message) error {
						return nil
					})
				}
				time.Sleep(time.Duration(RETRY_INTERVAL) * time.Millisecond)
			}()
		}

		// Clean-up the message body and reply to the sender
		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["messages"] = []float64{}

		for value := range values {
			body["messages"] = append(body["messages"].([]float64), value)
		}
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Clean-up the message body and reply to the sender
		body["type"] = "topology_ok"
		delete(body, "topology")
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
