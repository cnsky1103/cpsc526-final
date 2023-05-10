#!/bin/bash

cd ../server/master
# go run master.go
go run master.go &> master_run.log &
