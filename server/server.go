package server

import (
	"context"
	"fmt"
	"log"
	"os"

	"bigtable/server/proto"

	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
)

type TabletServer struct {
	tablets map[string]memstore.MemStoreI
}

func (server *TabletServer) Load(
	ctx context.Context,
	request *proto.LoadRequest,
) (*proto.LoadResponse, error) {
	tabletName := request.TabletName

	path := GetFullPath(tabletName)
	// if is not in the memory, load the sstable
	_, exist := server.tablets[tabletName]
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
		server.tablets[tabletName] = memstore.NewMemStore()
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

	server.tablets[tabletName] = store
	return &proto.LoadResponse{}, nil
}

func MakeTabletServer() *TabletServer {
	return &TabletServer{
		tablets: make(map[string]memstore.MemStoreI),
	}
}

// type TabletServerImp struct {
// 	nodeName string
// }

// func (server *TabletServer) Load(tabletName string) {
// 	path := GetFullPath(tabletName)
// 	// if is not in the memory, load the sstable
// 	_, exist := server.tablets[tabletName]
// 	if exist {
// 		return
// 	}

// 	// if is not exist in the disk, simply create an empty memtable
// 	if _, err := os.Stat(path); err == nil {
// 		fmt.Printf("folder exists\n")
// 	} else {
// 		fmt.Printf("folder does not exist\n")
// 		if err := os.MkdirAll(path, os.ModePerm); err != nil {
// 			log.Fatal(err)
// 		}
// 		server.tablets[tabletName] = memstore.NewMemStore()
// 		return
// 	}

// 	reader, err := sstables.NewSSTableReader(
// 		sstables.ReadBasePath(path),
// 		sstables.ReadWithKeyComparator(skiplist.BytesComparator),
// 	)

// 	if err != nil {
// 		log.Fatalf("error: %v", err)
// 	}
// 	defer reader.Close()

// 	metadata := reader.MetaData()
// 	log.Printf("reading table with %d records, minKey %d and maxKey %d", metadata.NumRecords, metadata.MinKey, metadata.MaxKey)

// 	iter, err := reader.Scan()
// 	if err != nil {
// 		log.Fatalf("error: %v\n", err)
// 	}

// 	numRecords := int(reader.MetaData().NumRecords)

// 	store := memstore.NewMemStore()
// 	for i := 0; i < numRecords; i++ {
// 		key, value, err := iter.Next()
// 		if err != nil {
// 			break
// 		}
// 		store.Add(key, value)
// 	}

// 	server.tablets[tabletName] = store
// }

// type Server struct {
// 	ms   memstore.MemStoreI
// 	path string
// }

func (server *TabletServer) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	key := request.GetKey()
	tabletName := "tablet1"
	data, err := server.tablets[tabletName].Get([]byte(key))
	if err != nil {
		_ = fmt.Errorf("Cannot get value: %v\n", err)
	}
	return &proto.GetResponse{
		Value: string(data),
	}, nil
}

// func (server *TabletServer) Get(key string) string {
// 	// server.ms.Get()

// 	// First, we need to contact etcd for tablet path by key

// 	tabletName := "tablet1"
// 	data, err := server.tablets[tabletName].Get([]byte(key))
// 	if err != nil {
// 		_ = fmt.Errorf("Cannot get value: %v\n", err)
// 	}
// 	return string(data)
// }

func (server *TabletServer) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	key := request.GetKey()
	value := request.GetValue()

	tabletName := "tablet1"
	err := server.tablets[tabletName].Add([]byte(key), []byte(value))
	if err != nil {
		_ = fmt.Errorf("Cannot set value: %v", err)
		return &proto.SetResponse{}, err
	}

	return &proto.SetResponse{}, nil
}

// func (server *TabletServer) Set(key string, value string) {
// 	// server.ms.Add()
// 	tabletName := "tablet1"
// 	err := server.tablets[tabletName].Add([]byte(key), []byte(value))
// 	if err != nil {
// 		_ = fmt.Errorf("Cannot set value: %v", err)
// 	}
// }

func (server *TabletServer) Flush() {
	for tabletName, tablet := range server.tablets {
		path := GetFullPath(tabletName)
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
