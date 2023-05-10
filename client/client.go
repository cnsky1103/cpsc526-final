package client

import (
	"bigtable/config"
	"bigtable/server/proto"
	"context"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Client struct {
	etcdCli     *clientv3.Client
	master_conn proto.MasterServiceClient
}

func NewClient() *Client {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	return &Client{
		etcdCli: cli,
	}
}

func (client *Client) GetTabletForKey(key string) string {
	root_tablet, err := client.etcdCli.Get(context.Background(), "root_tablet")

	if err != nil {

	}

	/*
		find root tablet server, get the user table for key
		return user table address
	*/

	return ""
}

func (client *Client) Get(key string) string {
	tablet_path := client.GetTabletForKey(key)
	/*
		call server.Get()
	*/
	return ""
}

func (client *Client) Put(key string, val string) string {
	tablet_path := client.GetTabletForKey(key)
	/*
		call server.Get()
	*/
	return ""
}
