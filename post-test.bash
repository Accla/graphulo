#!/bin/bash
DIR=$( cd "$(dirname "$0")" ; pwd -P )
#ls -l .
#ls -l ./shippable
#ls -l ./shippable/testresults
#echo "Working directory: $DIR"
for i in $( ls "$DIR/shippable/testresults/"*-output.txt ); do
    echo "TEST OUTPUT FILE: $i"
    cat "$i"
done
