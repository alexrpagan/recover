#!/bin/bash

for i in `pgrep -f 'xfer'`; do kill $i; done