#!/bin/bash

./start_etcd.sh
echo "Starting master";
./start_master.sh && sleep 2s;

echo "Starting tablet server";
./start_server.sh && sleep 2s;

echo "Running simple client test"
./start_simple_client.sh

./cleanup.sh