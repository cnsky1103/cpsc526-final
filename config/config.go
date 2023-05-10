package config

import "time"

const (
	DialTimeout        = 5 * time.Second
	RequestTimeout     = 10 * time.Second
	EtcdIp             = "127.0.0.1:2379"
	MasterServerIp     = "127.0.0.1:1919"
	TabletFlushMaxSize = 100
	TabletSplitMaxSize = 50
)

var (
	Endpoints = []string{"localhost:2379"}
)
