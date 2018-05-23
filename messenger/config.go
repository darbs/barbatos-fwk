package messenger

import "time"

type Message struct {
	Data string `json:"data"`
}

type Config struct {
	Url       string
	Durable   bool
	Attempts  int
	Delay     time.Duration
	Threshold uint32
}