package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	values := make(map[float64]bool)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		value := body["message"].(float64)
		if _, existed := values[value]; existed {
			return nil
		}
		values[value] = true

		// Broadcast value to all nodes in the network
		for _, value := range n.NodeIDs() {
			n.RPC(value, msg.Body, func(msg maelstrom.Message) error {
				return nil
			})
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
