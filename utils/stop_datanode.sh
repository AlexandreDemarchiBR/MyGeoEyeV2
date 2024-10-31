#!/bin/bash

# Check if the PID file exists
if [ -f datanode.pid ]; then
    # Read the PID from the file
    PID=$(cat datanode.pid)
    echo "Stopping datanode server with PID $PID..."
    
    # Kill the process
    kill $PID
    
    # Remove the PID file
    rm datanode.pid

    echo "datanode server stopped."
else
    echo "No PID file found. Is the datanode server running?"
    pkill -f datanode.py
fi
