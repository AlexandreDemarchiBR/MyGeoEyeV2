#!/bin/bash

# Checa se a quantidade de parametros está correta
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 {start|stop|update} {main|datanode|all}"
    exit 1
fi

# Assign arguments to variables
ACTION=$1
TARGET=$2

# Mata o processo do servidor main
stop_main() {
    echo "Stopping main service"
    # Add actual stop command for main here
}

# Mata todos os processos de datanodes
stop_datanode() {
    echo "Stopping datanode service"
    for ip in $(cat ../main_dir/workers.txt| cut -f 1 -d " "); do
        echo "Stopping datanode on $ip"
    done
}

# Mata todos os processos do sistema
stop_all() {
    echo "Stopping all services"
    echo "Stopping main on $ip"
    for ip in $(cat ../main_dir/workers.txt| cut -f 1 -d " "); do
        echo "Stopping datanode on $ip"
    done
}

# Inicia uma instância do servidor main
start_main() {
    echo "Starting main service"
    # Add actual start command for main here
}

# Inicia instâncias de todos os datanodes
start_datanode() {
    for ip in $(cat ../main_dir/workers.txt| cut -f 1 -d " "); do
        echo "Starting datanode on $ip"
    done
}

# Inicia instâncias de todo o sistema
start_all() {
    echo "Starting all services"
    echo "Starting main on $ip"
    for ip in $(cat ../main_dir/workers.txt| cut -f 1 -d " "); do
        echo "Starting datanode on $ip"
    done
    # Add actual start command for all services here
}

# Atualiza os arquivos de main
update_main() {
    echo "Updating main service"
    # Add actual update command for main here
}

# Atualiza os arquivos do datanode
update_datanode() {
    echo "Update datanode service"
    for ip in $(cat ../main_dir/workers.txt| cut -f 1 -d " "); do
        echo "Update datanode on $ip"
    done
}

# Atualiza os arquivos de todo o sistema
update_all() {
    echo "Update main service"
    echo "Update datanode service"
    for ip in $(cat ../main_dir/workers.txt| cut -f 1 -d " "); do
        echo "Update datanode on $ip"
    done
}

# Logic for handling actions and targets
if [ "$ACTION" == "stop" ]; then
    if [ "$TARGET" == "main" ]; then
        stop_main
    elif [ "$TARGET" == "datanode" ]; then
        stop_datanode
    elif [ "$TARGET" == "all" ]; then
        stop_all
    else
        echo "Invalid target: $TARGET"
        exit 1
    fi
elif [ "$ACTION" == "start" ]; then
    if [ "$TARGET" == "main" ]; then
        start_main
    elif [ "$TARGET" == "datanode" ]; then
        start_datanode
    elif [ "$TARGET" == "all" ]; then
        start_all
    else
        echo "Invalid target: $TARGET"
        exit 1
    fi
elif [ "$ACTION" == "restart" ]; then
    if [ "$TARGET" == "main" ]; then
        update_main
    elif [ "$TARGET" == "datanode" ]; then
        update_datanode
    elif [ "$TARGET" == "all" ]; then
        update_all
    else
        echo "Invalid target: $TARGET"
        exit 1
    fi
elif [ "$ACTION" == "update" ]; then
    if [ "$TARGET" == "main" ]; then
        update_main
    elif [ "$TARGET" == "datanode" ]; then
        update_datanode
    elif [ "$TARGET" == "all" ]; then
        update_all
    else
        echo "Invalid target: $TARGET"
        exit 1
    fi
else
    echo "Invalid action: $ACTION"
    exit 1
fi
