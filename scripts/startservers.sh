#!/bin/bash
set -e

user="apagan"

i=0
while read host; do
    echo $i
    ssh $user@$host "/home/$user/code/xfertest/scripts/killservers.sh" &
    ssh $user@$host "nohup /home/$user/code/xfertest/src/main/xfer -me $i -hosts /home/$user/code/xfertest/scripts/servers" &
    ((i++))
done < servers

wait
