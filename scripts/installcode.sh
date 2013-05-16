#!/bin/bash
set -e

user="ubuntu"

while read host; do
    ssh -i 6824.pem $user@$host "mkdir -p code \
                      && cd code \
                      && rm -rf xfertest \
                      && git clone https://github.com/alexrpagan/xfertest.git \
                      && cd xfertest/src/main \
                      && GOPATH=../.. go build -o xfer" &
done < servers

wait
echo "done installing code"