#!/usr/bin/env sh

# run go test for k times
k=$1
for i in {1..k}; do
    go test --race
    # exit if test failed
    if [ $? -ne 0 ]; then
        exit 1
    fi
done
