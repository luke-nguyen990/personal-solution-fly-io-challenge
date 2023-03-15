package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n *maelstrom.Node
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var response map[string]any
	if err := json.Unmarshal(msg.Body, &response); err != nil {
		return err
	}
	response["type"] = "echo_ok"
	return s.n.Reply(msg, response)
}

func main() {

	n := maelstrom.NewNode()
	s := &server{n}

	go func() {
		n.Handle("echo", s.readHandler)
	}()

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
