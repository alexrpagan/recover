#!/bin/bash
set -e

host1="istc"
host2=".csail.mit.edu"
user="apagan"

for idx in {1..12}
do
    host="$host1$idx$host2";
    echo "Asking $host"
    echo "---------------------------"
    ssh $host -l $user "finger"
    echo
done
