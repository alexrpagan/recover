#!/bin/bash

for i in `pgrep -f 'xfer'`; do kill -9 $i; done