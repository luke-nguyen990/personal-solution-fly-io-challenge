package main

import (
	"encoding/json"
	"log"

	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n      *maelstrom.Node
	values []float64
}

func (s *server) HandleRead(msg maelstrom.Message) error {
	var request map[string]any
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return err

	}
	var response = map[string]any{
		"type":     "read_ok",
		"messages": s.values,
	}
	return s.n.Reply(msg, response)
}

func (s *server) HandleBroadcast(msg maelstrom.Message) error {
	var request map[string]any
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return err
	}
	s.values = append(s.values, request["message"].(float64))
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
	s := &server{n: n}

	go func() {
		n.Handle("broadcast", s.HandleBroadcast)
	}()

	go func() {
		n.Handle("read", s.HandleRead)
	}()

	go func() {
		n.Handle("topology", s.HandleTopology)
	}()

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
