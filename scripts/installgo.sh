#!/bin/bash
set -e

user="ubuntu"


# while read host; do
#     scp -i 6824.pem /tmp/go.tar.gz $user@$host:~
# done < servers

# while read host; do
#     ssh -i 6824.pem $user@$host "sudo apt-get -y install gcc git golang "&
# done < servers

while read host; do
    ssh -i 6824.pem $user@$host "echo export GOPATH=/home/ubuntu/code/xfertest/ >> .bashrc "&
done < servers


wait
echo "done installing"