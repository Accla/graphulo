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
HOST="f-2-12.llgrid.ll.mit.edu"
TABLENAME="test_table4321"
CBUSER="root"
PASSWORD="secret"
NUM_THREADS="2"
ROW_QUERY="ZOO10001"
set -x
java -classpath .:$CLASSPATH edu.mit.ll.d4m.db.cloud.TestScanner  $INSTANCE $HOST  $CBUSER $PASSWORD $TABLENAME $NUM_THREADS $ROW_QUERY



