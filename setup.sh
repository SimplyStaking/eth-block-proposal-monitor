#!/bin/bash

file=/etc/systemd/system/eth2_block_monitoring.service
if test -f "$file"; then
    while true; do
        read -p "Service file already exists. Would you like to replace it? [Y/N] " yn
        case $yn in
            [Yy]* ) break;;
            [Nn]* ) exit;;
            * ) echo "Please enter Y/N.";;
        esac
    done
fi

tee /etc/systemd/system/eth2_block_monitoring.service > /dev/null <<EOF  
[Unit]
Description     = ETH2 Block Monitoring Service
Wants           = network-online.target 

[Service]
User              = monitoring
WorkingDirectory  = /home/monitoring/eth-block-proposal-monitor/src/
ExecStart         = python3 -u /home/monitoring/eth-block-proposal-monitor/src/main.py
Restart           = always
RestartSec        = 5s

[Install]
WantedBy= multi-user.target
EOF

systemctl daemon-reload
systemctl enable eth2_block_monitoring
systemctl start eth2_block_monitoring
