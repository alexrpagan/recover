#!/bin/bash
set -e

user="apagan"

while read host; do
    ssh $user@$host "mkdir -p code \
                      && cd code \
                      && rm -rf xfertest \
                      && git clone https://github.com/alexrpagan/xfertest.git \
                      && cd xfertest/src/main \
                      && GOPATH=../.. ~/go/bin/go build -o xfer" &
done < servers

wait
echo "done installing code"