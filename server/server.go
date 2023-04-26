package server

import (
	"fmt"
	"log"
	"os"

	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
)

type TabletServer struct {
	tablets map[string]memstore.MemStoreI
}

func MakeTabletServer() *TabletServer {
	return &TabletServer{
		tablets: make(map[string]memstore.MemStoreI),
	}
}

func (server *TabletServer) Load(path string) {
	// if is not in the memory, load the sstable
	_, exist := server.tablets[path]
	if exist {
		return
	}

	// if is not exist in the disk, simply create an empty memtable
	if _, err := os.Stat(path); err == nil {
		fmt.Printf("File exists\n")
	} else {
		fmt.Printf("File does not exist\n")
		server.tablets[path] = memstore.NewMemStore()
		return
	}

	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(path),
		sstables.ReadWithKeyComparator(skiplist.BytesComparator))
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

	server.tablets[path] = store
}

// type Server struct {
// 	ms   memstore.MemStoreI
// 	path string
// }

func (server *TabletServer) Get(key string) string {
	// server.ms.Get()
	return ""
}

func (server *TabletServer) Set(key string, value string) {
	// server.ms.Add()
	server.tablets["/Users/ybyan/Documents/GitHub/cpsc526-final/files"].Add([]byte{1}, []byte{1})
}

func (server *TabletServer) Flush() {
	for path, tablet := range server.tablets {
		err := tablet.Flush(sstables.WriteBasePath(path))
		if err != nil {
			log.Fatalf("Fatal %v\n", err)
		}
	}
}
