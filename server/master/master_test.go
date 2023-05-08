package main

import (
	"bigtable/server/proto"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMasterGetKey(t *testing.T) {
	master, _ := MakeMasterServer()
	context := context.Background()
	resp, err := master.GetTabletByKey(context, &proto.GetTabletByKeyRequest{
		Key: "Empty",
	})

	assert.Nil(t, err)
	_ = resp.TabletName
}
