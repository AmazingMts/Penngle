#!/bin/bash

# Function to cleanup existing processes
cleanup_processes() {
    echo "Cleaning up existing processes..."
    pkill -f "cis5550.kvs.Coordinator"
    pkill -f "cis5550.kvs.Worker"
    pkill -f "cis5550.flame.Coordinator"
    pkill -f "cis5550.flame.Worker"
    pkill -f "cis5550.jobs.Query"
    sleep 2
    echo "Cleanup complete"
}

# Clean up processes before starting
cleanup_processes

# Clean up running files
rm -rf worker* *.jar run_*.sh