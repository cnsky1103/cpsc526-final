package config

import "time"

const (
	DialTimeout    = 5 * time.Second
	RequestTimeout = 10 * time.Second
)

var (
	Endpoints = []string{"localhost:2379"}
)
