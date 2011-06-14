#!/bin/sh
#################################
# test table split
#################################
PRG=$0
PRGDIR=`dirname "$PRG"`
H=`cd "$PRGDIR/.." ; pwd`
export BASEDIR=$H
. $H/bin/set-classpath.sh

INSTANCE="cloudbase"
HOST="f-2-15.llgrid.ll.mit.edu"
TABLENAME="test_table4321"
CBUSER="root"
PASSWORD="secret"
NUM_ENTRIES="6000000"
NUM_PARTITION="500000"
## RECORD_PREFIX is appended to the row key (startVertexString)
RECORD_PREFIX="ZOO"

set -x
java -classpath .:$CLASSPATH edu.mit.ll.d4m.db.cloud.TestTableSplit $HOST $INSTANCE $TABLENAME $CBUSER $PASSWORD $NUM_ENTRIES $NUM_PARTITION $RECORD_PREFIX



