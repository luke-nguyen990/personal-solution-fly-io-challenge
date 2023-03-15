package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	HandleRead      string = "read"
	HandleBroadcast string = "broadcast"
	HandleTopology  string = "topology"
)

type server struct {
	n      *maelstrom.Node
	values map[float64]bool
}

func (s *server) HandleRead(msg maelstrom.Message) error {
	var request map[string]any
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return err
	}
	var messages []float64
	for key := range s.values {
		messages = append(messages, key)
	}
	var response = map[string]any{
		"type":     "read_ok",
		"messages": messages,
	}
	return s.n.Reply(msg, response)
}

func (s *server) HandleBroadcast(msg maelstrom.Message) error {
	var request map[string]any
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return err
	}
	if _, existed := s.values[request["message"].(float64)]; existed {
		return nil
	}
	s.values[request["message"].(float64)] = true
	for _, dest := range s.n.NodeIDs() {
		s.n.RPC(dest, msg.Body, func(msg maelstrom.Message) error {
			return nil
		})
	}
	var response = map[string]any{
		"type": "broadcast_ok",
	}
	return s.n.Reply(msg, response)
}

func (s *server) HandleTopology(msg maelstrom.Message) error {
	var request map[string]any

	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return err
	}
	var response = map[string]any{
		"type": "topology_ok",
	}
	return s.n.Reply(msg, response)
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, values: map[float64]bool{}}

	n.Handle(HandleBroadcast, s.HandleBroadcast)
	n.Handle(HandleRead, s.HandleRead)
	n.Handle(HandleTopology, s.HandleTopology)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}