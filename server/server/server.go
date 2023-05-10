package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"bigtable/config"
	"bigtable/server"
	"bigtable/server/proto"

	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type TabletServer struct {
	proto.UnimplementedTabletServiceServer
	tablets   map[string]memstore.MemStoreI
	masterCli proto.MasterServiceClient
}

func (this *TabletServer) Load(
	ctx context.Context,
	request *proto.LoadRequest,
) (*proto.LoadResponse, error) {
	tabletName := request.TabletName

	path := server.GetFullPath(tabletName)
	// if is not in the memory, load the sstable
	_, exist := this.tablets[tabletName]
	if exist {
		return &proto.LoadResponse{}, nil
	}

	// if is not exist in the disk, simply create an empty memtable
	if _, err := os.Stat(path); err == nil {
		fmt.Printf("folder exists\n")
	} else {
		fmt.Printf("folder does not exist\n")
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Fatal(err)
		}
		this.tablets[tabletName] = memstore.NewMemStore()
		return &proto.LoadResponse{}, nil
	}

	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(path),
		sstables.ReadWithKeyComparator(skiplist.BytesComparator),
	)

	if err != nil {
		log.Fatalf("error: %v", err)
	}
	defer reader.Close()

	metadata := reader.MetaData()
	log.Printf("reading table with %d records, minKey %d and maxKey %d", metadata.NumRecords, metadata.MinKey, metadata.MaxKey)

	iter, err := reader.Scan()
	if err != nil {
		log.Fatalf("error: %v\n", err)
	}

	numRecords := int(reader.MetaData().NumRecords)

	store := memstore.NewMemStore()
	for i := 0; i < numRecords; i++ {
		key, value, err := iter.Next()
		if err != nil {
			break
		}
		store.Add(key, value)
	}

	this.tablets[tabletName] = store
	return &proto.LoadResponse{}, nil
}

func MakeTabletServer() (*TabletServer, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	masterConn, err := grpc.Dial(config.MasterServerIp, opts...)
	if err != nil {
		return nil, err
	}
	masterCli := proto.NewMasterServiceClient(masterConn)
	return &TabletServer{
		tablets:   make(map[string]memstore.MemStoreI),
		masterCli: masterCli,
	}, nil
}

func (server *TabletServer) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	key := request.GetKey()
	response, err := server.masterCli.GetTabletByKey(ctx, &proto.GetTabletByKeyRequest{Key: key})
	if err != nil {
		return &proto.GetResponse{}, err
	}
	tabletName := response.GetTabletName()
	data, err := server.tablets[tabletName].Get([]byte(key))
	if err != nil {
		_ = fmt.Errorf("Cannot get value: %v\n", err)
	}
	return &proto.GetResponse{
		Value: string(data),
	}, nil
}

func (server *TabletServer) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	key := request.GetKey()
	value := request.GetValue()

	response, err := server.masterCli.GetTabletByKey(ctx, &proto.GetTabletByKeyRequest{Key: key})
	if err != nil {
		return &proto.SetResponse{}, err
	}
	tabletName := response.GetTabletName()

	tablet, exist := server.tablets[tabletName]
	if !exist {
		return &proto.SetResponse{}, status.Errorf(codes.NotFound, "TABLET_NOT_FOUND")
	}

	// if tablet.Size()

	err = tablet.Add([]byte(key), []byte(value))
	if err != nil {
		_ = fmt.Errorf("Cannot set value: %v", err)
		return &proto.SetResponse{}, err
	}

	return &proto.SetResponse{}, nil
}

func (server *TabletServer) Ping(
	ctx context.Context,
	request *proto.PingRequest,
) (*proto.PingResponse, error) {
	// A dummy implementation.
	return &proto.PingResponse{
		IsAlive: true,
	}, nil
}

func (this *TabletServer) Flush() {
	for tabletName, tablet := range this.tablets {
		path := server.GetFullPath(tabletName)
		os.RemoveAll(path)
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Fatal(err)
		}
		err := tablet.Flush(sstables.WriteBasePath(path))
		if err != nil {
			log.Fatalf("Fatal %v\n", err)
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})

	if err != nil {
		log.Fatalf("failed to establish connection with etcd: %v", err)
	}

	curServersRes, err := etcdCli.Get(context.Background(), "servers")

	if err != nil {
		log.Fatalf("failed to get servers: %v", err)
	}

	log.Println(lis.Addr().String())

	var curServers string
	if len(curServersRes.Kvs) == 0 {
		curServers = ""
	} else {
		curServers = string(curServersRes.Kvs[0].Value)
	}
	// 127.0.0.1:1111;127.0.0.1:2222;
	etcdCli.Put(context.Background(), "servers", curServers+lis.Addr().String()+";")

	s := grpc.NewServer()
	server, err := MakeTabletServer()

	if err != nil {
		log.Fatalf("failed to MakeTabletServer: %q", err)
	}

	proto.RegisterTabletServiceServer(s, server)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
