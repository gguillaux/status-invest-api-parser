#!/bin/bash

# check if $1 is not empty, if it is empty, set it to 5
if [ -z "$1" ]; then
    ASSETS=7
else
    ASSETS=$1
fi

for i in $(ls carteiras/*.txt); do
    python main.py $i $ASSETS
done