package main

import (
	"bigtable/config"
	"bigtable/server/proto"
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MasterServer struct {
	proto.UnimplementedMasterServiceServer
	cli          *clientv3.Client
	servers      []string // store all server ips
	server_conns map[string]proto.TabletServiceClient

	tablets     []string          // store all tablets existing
	assignments map[string]string // k: tablet name; v: server ip
	// tablets_for_table map[string][]string // k: table, v: tablets for this table
}

func MakeMasterServer() (*MasterServer, error) {
	server := &MasterServer{
		servers:      make([]string, 0),
		server_conns: make(map[string]proto.TabletServiceClient),
		tablets:      make([]string, 0),
		assignments:  make(map[string]string),
	}

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

			var opts []grpc.DialOption
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

			// Establish Connection
			log.Printf("Servers: %v\n", this.servers)
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
	}
}

func (this *MasterServer) serversLivenessDetection() {
	for _, ip := range this.servers {
		// TODO: res, err := this.server_conns[ip].Ping()
		res, err := this.server_conns[ip].Ping(context.Background(), &proto.PingRequest{})

		if !res.IsAlive || err != nil {
			log.Printf("Ping server %v failed or server is not alive. Err: %v\n", ip, err)
			// delete this server's information
			this.servers = removeElement(this.servers, ip)

			// NOT SURE: close the corresponding connection
			// this.server_conns[ip].close()

			// change the etcd "servers", remove the dead ip
			// if the serversStr is "1.1.1.1;2.2.2.2;3.3.3.3;"
			// and we want to remove 2.2.2.2
			// we split it and re-construct it
			serversRes, _ := this.cli.Get(context.Background(), "servers")
			serversStr := string(serversRes.Kvs[0].Value)
			serverIpsStr := strings.Split(serversStr, ";")
			newServers := ""
			for _, oldIp := range serverIpsStr {
				if ip != oldIp {
					newServers += oldIp + ";"
				}
			}
			this.cli.Put(context.Background(), "servers", newServers)

			// re-assign tablets
			for k, v := range this.assignments {
				if v == ip {
					// re-assign k to another server
					re_assignment_index := rand.Int() % len(this.servers)
					re_assignment_ip := this.servers[re_assignment_index]
					this.assignments[k] = re_assignment_ip
					this.RequestTabletServerLoading(re_assignment_ip, k)

					// notify etcd about the change
					this.cli.Put(context.Background(), k, re_assignment_ip)
				}
			}
		}
	}
}

func removeElement(slice []string, element string) []string {
	for i := 0; i < len(slice); i++ {
		if slice[i] == element {
			// Remove the element by slicing the slice
			slice = append(slice[:i], slice[i+1:]...)
			// Decrement the index to adjust for the removed element
			i--
		}
	}
	return slice
}

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
			log.Printf("Failed to RequestTabletServerLoading: %v\n", err)
			return &proto.GetTabletByKeyResponse{}, err
		}
		this.assignments[name] = server_to_load
		// Log assignments to the etcd.
		this.cli.Put(context.Background(), name, server_to_load)
		ip = server_to_load
	}

	// if !exist {
	// 	return &proto.GetTabletByKeyResponse{}, status.Errorf(codes.NotFound, "SERVER_NOT_AVAILABLE")
	// }

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

	// Clean init process
	server.cli.Put(context.Background(), "servers", "")

	// Handling Clean Up Jobs
	go func(server *MasterServer) {
		ctx, stop := signal.NotifyContext(
			context.Background(),
			os.Interrupt,    // SIGINT, Ctrl+C
			syscall.SIGTERM, // systemd
		)

		for {
			select {
			case <-time.After(10 * time.Second):
				continue
			case <-ctx.Done():
				stop()
				log.Println("signal received")
				log.Println("Start to clean up...")
				server.cli.Put(context.Background(), "servers", "")
				os.Exit(1)
			}
		}
	}(server)

	proto.RegisterMasterServiceServer(s, server)
	log.Printf("[master] server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
