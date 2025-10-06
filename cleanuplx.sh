#!/bin/bash

function cleanup_processes() {
    echo "Cleaning up existing processes..."
    pkill -f "cis5550.kvs.Coordinator"
    pkill -f "cis5550.kvs.Worker"
    pkill -f "cis5550.flame.Coordinator"
    pkill -f "cis5550.flame.Worker"
    sleep 2
    echo "Cleanup complete"
}

cleanup_processes

# Clean up files
rm -rf worker* *.jar run_*.sh