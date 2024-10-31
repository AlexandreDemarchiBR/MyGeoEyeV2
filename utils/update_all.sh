#!/bin/bash
# envia arqivos para os workers
for ip in $(cat main_dir/workers.txt| cut -f 1 -d " "); do
    expect utils/update_host.exp main.py $ip
    expect utils/update_host.exp datanode.py $ip
    expect utils/update_host.exp main_dir/workers.txt $ip

    expect utils/update_host.exp update_all $ip
    expect utils/update_host.exp utils/update_all.sh $ip
    expect utils/update_host.exp utils/update_host.exp $ip

    expect utils/update_host.exp setup_all $ip
    expect utils/update_host.exp utils/setup_all.sh $ip
    expect utils/update_host.exp utils/setup_host.exp $ip

    expect utils/update_host.exp service $ip
    expect utils/update_host.exp utils/service.sh $ip
    expect utils/update_host.exp utils/control_host.exp  $ip
    expect utils/update_host.exp utils/start_main.sh  $ip
    expect utils/update_host.exp utils/stop_main.sh  $ip
    expect utils/update_host.exp utils/start_datanode.sh  $ip
    expect utils/update_host.exp utils/stop_datanode.sh  $ip


    echo '###########################################'
    echo
done