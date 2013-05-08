#!/bin/bash

for i in `pgrep -f 'go run main.go'`; do kill $i; done