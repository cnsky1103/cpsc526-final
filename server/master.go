package server

import (
	"bigtable/config"
	"context"
	"log"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type MasterServer struct {
	cli     *clientv3.Client
	servers []string // store all server ips

	// tablets []string // store all tablets existing
	// tablets_for_table map[string][]string // k: table, v: tablets for this table
}

func MakeMasterServer() *MasterServer {
	server := &MasterServer{}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})

	if err != nil {
		return nil
	}

	server.cli = cli

	go server.watchServers()

	return server
}

func (this *MasterServer) watchServers() {
	rch := this.cli.Watch(context.Background(), "servers")
	for wresp := range rch {
		for _, ev := range wresp.Events {
			log.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			this.servers = strings.Split(string(ev.Kv.Value), ";")
		}
	}
}
