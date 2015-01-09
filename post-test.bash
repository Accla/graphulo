#!/bin/bash
DIR=$( cd "$(dirname "$0")" ; pwd -P )
for i in $( ls "$DIR/shippable/testresults/"*-output.txt ); do
    echo FILE: $i
    cat $i
done
