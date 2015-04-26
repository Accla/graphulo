#!/bin/bash
DIR=$( cd "$(dirname "$0")" ; pwd -P )
#echo "Working directory: $DIR"
for i in $( ls "$DIR/shippable/testresults/"*-output.txt ); do
    echo "TEST OUTPUT FILE: $i"
    cat "$i"
done
