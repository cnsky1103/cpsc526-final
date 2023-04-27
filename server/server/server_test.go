package main

import (
	"bigtable/server/proto"
	"context"
	"log"
	"testing"

	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/sstables"
)

func TestTabletLoad(t *testing.T) {
	server, _ := MakeTabletServer()
	context := context.Background()
	server.Load(context, &proto.LoadRequest{TabletName: "tablet1"})
	// server.Load("tablet1")
	server.Set(context, &proto.SetRequest{
		Key:   "Hello",
		Value: "World",
	})
	// server.Set("Hello", "World")
	server.Flush()
}

func TestTabletGet(t *testing.T) {
	server, _ := MakeTabletServer()
	context := context.Background()
	server.Load(context, &proto.LoadRequest{TabletName: "tablet1"})
	// server.Load("tablet1")
	response, err := server.Get(context, &proto.GetRequest{Key: "Hello"})
	// value := server.Get("Hello")
	if err != nil {
		t.Errorf("Error Get: %v\n", err)
	}
	if response.GetValue() != "World" {
		t.Errorf("Failed.")
	}
}

func TestCreateSSTable(t *testing.T) {
	path := "/Users/ybyan/Documents/GitHub/cpsc526-final/files"
	// defer os.RemoveAll(path)

	ms := memstore.NewMemStore()
	ms.Add([]byte{1}, []byte{1})
	ms.Add([]byte{2}, []byte{2})
	ms.Upsert([]byte{1}, []byte{2})
	ms.Delete([]byte{2})
	ms.DeleteIfExists([]byte{3})
	value, _ := ms.Get([]byte{1})
	log.Printf("value for key 1: %d", value) // yields 2

	size := ms.EstimatedSizeInBytes()
	log.Printf("memstore size in bytes: %d", size) // yields 3

	ms.Flush(sstables.WriteBasePath(path))
}
