package server

import "testing"

func TestTabletLoad(t *testing.T) {
	server := MakeTabletServer()
	server.Load("/Users/ybyan/Documents/GitHub/cpsc526-final/files")
	server.Set("", "")
	server.Flush()
}
