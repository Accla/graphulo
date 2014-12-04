#!/bin/bash
for i in $( ls shippable/testresults/*-output.txt ); do
    echo FILE: $i
    cat $i
done