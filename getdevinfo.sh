#!/bin/bash

# Start IP address and end IP address settings
start_ip="192.168.111"
end_ip="192.168.111"

# Function to connect to host via SSH and execute docker ps command
function ssh_and_get_docker_ps {
    ip=$1
    echo "Connecting to $ip..."
    # Connect to host via SSH and execute docker ps command
    ssh admin@$ip "docker ps | awk '/dev/ || /0\./{print \$NF \" -> \" \$2}'"
}

# Execute docker ps command for all IPs from start_ip to end_ip
for ((i=11; i<=17; i++))
do
    ip="$start_ip.$i"
    echo " "
    echo "----------------------------------------------------------------"
    echo " "
    ssh_and_get_docker_ps $ip
done

read -p "Press Enter to continue"