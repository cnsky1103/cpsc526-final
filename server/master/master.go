package main

import (
	"bigtable/config"
	"bigtable/server/proto"
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type MasterServer struct {
	proto.UnimplementedMasterServiceServer
	cli          *clientv3.Client
	servers      []string // store all server ips
	server_conns map[string]proto.TabletServiceClient

	tablets     []string // store all tablets existing
	assignments map[string]string
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

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Establish Connection
	for _, serverIp := range this.servers {
		if _, exist := this.server_conns[serverIp]; !exist {
			conn, err := grpc.Dial(serverIp, opts...)
			if err != nil {
				log.Printf("Err %v\n", err)
			} else {
				log.Printf("Established connection to %v\n", serverIp)
				this.server_conns[serverIp] = proto.NewTabletServiceClient(conn)
			}
		}
	}
}

// func (this *MasterServer) removedIp() []string {
// 	for
// }

func (this *MasterServer) GetTabletByKey(ctx context.Context, req *proto.GetTabletByKeyRequest) (*proto.GetTabletByKeyResponse, error) {
	name := "tablet_"
	for i := 0; i < len(this.tablets); i++ {
		if i == len(this.tablets)-1 {
			name += this.tablets[i]
		} else {
			if this.tablets[i] >= req.Key && this.tablets[i+1] < req.Key {
				name += this.tablets[i]
			}
		}
	}

	ip, exist := this.assignments[name]
	rand.Seed(time.Now().UnixNano())

	if !exist {
		index := rand.Int() % len(this.servers)
		server_to_load := this.servers[index]
		err := this.RequestTabletServerLoading(server_to_load, name)
		if err != nil {
			return &proto.GetTabletByKeyResponse{}, err
		}
		this.assignments[name] = server_to_load
		ip = server_to_load
	}

	if !exist {
		return &proto.GetTabletByKeyResponse{}, status.Errorf(codes.NotFound, "SERVER_NOT_AVAILABLE")
	}

	return &proto.GetTabletByKeyResponse{
		TabletName: name,
		ServerIp:   ip,
	}, nil
}

func (this *MasterServer) RequestTabletServerLoading(ip string, tabletName string) error {
	client, exist := this.server_conns[ip]
	if !exist {
		return errors.New("no established connection")
	}

	_, err := client.Load(context.Background(), &proto.LoadRequest{TabletName: tabletName})
	if err != nil {
		return err
	}
	return nil
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
