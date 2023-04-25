package server

import (
	"github.com/thomasjungblut/go-sstables/memstore"
)

type TabletServer struct {
	inmem memstore.MemStore
}

func (server *TabletServer) load(path string) {
	
}
