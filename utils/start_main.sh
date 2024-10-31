#!/bin/bash

HOST=0.0.0.0
PORT=5555
echo "Starting Main server on $HOST:$PORT..."

echo Killing previous process
pkill -f main.py

echo Starting main.py
python3 main.py &
disown

echo $! > main.pid
echo "Main server started with PID $(cat main.pid)"
#sleep 1 # resolve o $ não aparecendo (precisamos para o expect)
