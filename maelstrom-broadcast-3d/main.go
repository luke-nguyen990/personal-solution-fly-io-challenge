package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	HandleRead      string = "read"
	HandleBroadcast string = "broadcast"
	HandleTopology  string = "topology"
)

const (
	MaxRoutinesPerBroadcastOp = 1
	Interval                  = time.Duration(8000) * time.Millisecond
)

type server struct {
	n      *maelstrom.Node
	values map[float64]struct{}

	mutex sync.RWMutex
}

func (s *server) HandleRead(msg maelstrom.Message) error {
	var messages []float64
	s.mutex.Lock()
	for key := range s.values {
		messages = append(messages, key)
	}
	s.mutex.Unlock()
	var response = map[string]any{
		"type":     "read_ok",
		"messages": messages,
	}
	return s.n.Reply(msg, response)
}

func (s *server) HandleBroadcast(msg maelstrom.Message) error {
	type broadcastMsg struct {
		Type          string              `json:"type"`
		Message       float64             `json:"message,omitempty"`
		Broadcast     map[string][]string `json:"Broadcast,omitempty"`
		IsDestination string              `json:"isDestination"`
	}
	var request broadcastMsg
	if err := json.Unmarshal(msg.Body, &request); err != nil {
		return err
	}
	if request.IsDestination == "true" {
		s.mutex.Lock()
		s.values[request.Message] = struct{}{}
		s.mutex.Unlock()
		return nil
	}
	s.mutex.Lock()
	s.values[request.Message] = struct{}{}
	s.mutex.Unlock()

	go func() {
		for i := 0; i < MaxRoutinesPerBroadcastOp; i++ {
			for _, dest := range s.n.NodeIDs() {
				if dest == s.n.ID() {
					continue
				}
				request.IsDestination = "true"
				s.n.RPC(dest, request, func(msg maelstrom.Message) error {
					return nil
				})
			}
			time.Sleep(Interval)
		}
	}()

	var response = map[string]any{
		"type": "broadcast_ok",
	}
	return s.n.Reply(msg, response)
}

func (s *server) HandleTopology(msg maelstrom.Message) error {
	type topologyMsg struct {
		Type     string              `json:"type"`
		Topology map[string][]string `json:"topology,omitempty"`
	}
	var request topologyMsg
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
	s := &server{n: n, values: map[float64]struct{}{}, mutex: sync.RWMutex{}}

	n.Handle(HandleBroadcast, s.HandleBroadcast)
	n.Handle(HandleRead, s.HandleRead)
	n.Handle(HandleTopology, s.HandleTopology)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
