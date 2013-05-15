#!/bin/bash

for i in `pgrep -f 'code/xfertest/src/main/main'`; do kill $i; done