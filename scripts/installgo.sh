#!/bin/bash
set -e

user="apagan"


while read host; do
    scp /tmp/go.tar.gz $user@$host:~
done < servers

while read host; do
    ssh $user@$host "tar xzf go.tar.gz && cd go/src && ./all.bash" &
done < servers

wait
echo "done installing"