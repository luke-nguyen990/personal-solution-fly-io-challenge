package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n *maelstrom.Node
}

func (s *server) generateHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	var respose = map[string]any{
		"type": "generate_ok",
		"id":   uuid.New().String()}
	return s.n.Reply(msg, respose)
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n}

	go func() {
		n.Handle("generate", s.generateHandler)
	}()

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
