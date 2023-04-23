package server

import (
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/sstables"
)

type Server struct {
	ms   memstore.MemStoreI
	path string
}

func (server *Server) Get(key string) string {
	// server.ms.Get()
	return ""
}

func (server *Server) Set(key string, value string) {
	// server.ms.Add()
}

func (server *Server) Flush() {
	server.ms.Flush(sstables.WriteBasePath(server.path))
}
