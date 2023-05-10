#!/bin/bash

# Locate the etcd binary
etcd_binary=$(which etcd)

# Check if etcd is running
pgrep -x "etcd" >/dev/null

# If etcd is not running, start it
if [ $? -ne 0 ]; then
    if [ -n "$etcd_binary" ]; then
        echo "Starting etcd..."
        $etcd_binary >/dev/null 2>&1 &
        echo "Etcd has been started."
    else
        echo "Etcd binary not found. Please make sure it is installed and in your PATH."
    fi
else
    echo "Etcd is already running."
fi
