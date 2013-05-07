#!/bin/bash
set -e

host1="istc"
host2=".csail.mit.edu"
user="apagan"

# for idx in {1..6}
# do
#     host="$host1$idx$host2";
#     scp go.tar.gz $user@$host
# done

# wait
# echo "done uploading"


# for idx in {1..6}
# do
#     host="$host1$idx$host2";
#     ssh $user@$host "tar xzf go.tar.gz && cd go/src && export GOROOT=/home/apagan/install/go && ./all.bash" &
# done

# wait
# echo "done installing"