package main

import (
	"bigtable/config"
	"bigtable/server/proto"
	"context"
	"log"
	"net"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type MasterServer struct {
	proto.UnimplementedMasterServiceServer
	cli     *clientv3.Client
	servers []string // store all server ips

	tablets []string // store all tablets existing
	// tablets_for_table map[string][]string // k: table, v: tablets for this table
}

func MakeMasterServer() (*MasterServer, error) {
	server := &MasterServer{}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})

	if err != nil {
		return nil, err
	}

	server.cli = cli

	go server.watchServers()

	return server, nil
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

func (this *MasterServer) GetTabletByKey(ctx context.Context, req *proto.GetTabletByKeyRequest) (*proto.GetTabletByKeyResponse, error) {
	name := ""
	for i := 0; i < len(this.tablets); i++ {
		if i == len(this.tablets)-1 {
			name = this.tablets[i]
		} else {
			if this.tablets[i] >= req.Key && this.tablets[i+1] < req.Key {
				name = this.tablets[i]
			}
		}
	}
	return &proto.GetTabletByKeyResponse{TabletName: name}, nil
}

func main() {
	lis, err := net.Listen("tcp", config.MasterServerIp)
	if err != nil {
		log.Fatalf("[master] failed to listen: %v", err)
	}

	s := grpc.NewServer()
	server, err := MakeMasterServer()

	if err != nil {
		log.Fatalf("failed to MakeMasterServer: %q", err)
	}

	proto.RegisterMasterServiceServer(s, server)
	log.Printf("[master] server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
