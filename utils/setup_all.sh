#!/bin/bash

chmod +x service update_all setup_all *.py utils/*.sh utils/*exp
for ip in $(cat main_dir/workers.txt| cut -f 1 -d " "); do
    expect utils/setup_host.exp $ip
done
