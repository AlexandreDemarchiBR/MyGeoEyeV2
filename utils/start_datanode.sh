#!/bin/bash

HOST=0.0.0.0
PORT=5555
echo "Starting datanode server on $HOST:$PORT..."

echo Killing previous process
pkill -f datanode.py

echo Starting datanode.py
python3 datanode.py 0.0.0.0 6666 &
disown

echo $! > datanode.pid
echo "datanode server started with PID $(cat datanode.pid)"
#sleep 1 # resolve o $ não aparecendo (precisamos para o expect)
