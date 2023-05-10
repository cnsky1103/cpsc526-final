package client

import (
	"bigtable/config"
	"bigtable/server/proto"
	"context"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	etcdCli     *clientv3.Client
	master_conn proto.MasterServiceClient
}

func NewClient() (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.Dial(config.MasterServerIp, opts...)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &Client{
		etcdCli:     cli,
		master_conn: proto.NewMasterServiceClient(conn),
	}, nil
}

func (client *Client) GetTabletServerIpForKey(key string) (string, error) {
	// root_tablet, err := client.etcdCli.Get(context.Background(), "root_tablet")
	resp, err := client.master_conn.GetTabletByKey(
		context.Background(),
		&proto.GetTabletByKeyRequest{Key: key},
	)

	if err != nil {
		log.Printf("Failed to get tablet name from master server: %v\n", err)
		return "", err
	}

	return resp.ServerIp, nil
}

func (client *Client) Get(key string) (string, error) {
	serverIp, err := client.GetTabletServerIpForKey(key)

	if err != nil {
		log.Println(err)
		return "", err
	}

	tabletClient, err := establishTabletClient(serverIp)
	if err != nil {
		log.Println(err)
		return "", err
	}

	resp, err := tabletClient.Get(context.Background(), &proto.GetRequest{Key: key})

	if err != nil {
		log.Println(err)
		return "", err
	}

	return resp.Value, nil
}

func (client *Client) Set(key string, val string) error {
	serverIp, err := client.GetTabletServerIpForKey(key)

	if err != nil {
		log.Println(err)
		return err
	}

	tabletClient, err := establishTabletClient(serverIp)
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = tabletClient.Set(context.Background(), &proto.SetRequest{
		Key:   key,
		Value: val,
	})

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func establishTabletClient(ip string) (proto.TabletServiceClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.Dial(ip, opts...)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	tabletClient := proto.NewTabletServiceClient(conn)
	return tabletClient, nil
}
