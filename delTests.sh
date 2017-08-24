#!/bin/sh
${ACCUMULO_HOME}/bin/accumulo shell -u root -p secret -e "deletetable -p .*Test_test.* -f"
