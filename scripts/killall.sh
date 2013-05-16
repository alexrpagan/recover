#!/bin/bash
set -e

user="apagan"

echo "killing servers"
while read host; do
    ssh $user@$host ". /home/$user/code/xfertest/scripts/killservers.sh" &
done < servers
wait
