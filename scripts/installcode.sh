#!/bin/bash
set -e

user="apagan"

while read host; do
    ssh $user@$host " . .bash_profile \
                      && mkdir -p code
                      && cd code \
                      && rm -rf xfertest \
                      && git clone https://github.com/alexrpagan/xfertest.git \
                      && . ./scripts/init.sh \
                      && ./scripts/killservers.sh
                      && cd xfertest/src/main \
                      && go run main.go -h $host" &
done

wait
echo "done installing code"