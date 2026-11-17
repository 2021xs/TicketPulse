package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"ticketpulse/internal/ticket"
)

func LoadRouting(path string) (ticket.Routing, error) {
	f, err := os.Open(path)
	if err != nil {
		return ticket.Routing{}, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 64<<10))
	d.DisallowUnknownFields()
	var routing ticket.Routing
	if err := d.Decode(&routing); err != nil {
		return routing, fmt.Errorf("decode routing: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return routing, fmt.Errorf("routing must contain exactly one JSON object")
	}
	return routing, routing.Validate()
}
