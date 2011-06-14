#!/bin/sh
#################################
# test table split
#################################
PRG=$0
PRGDIR=`dirname "$PRG"`
H=`cd "$PRGDIR/.." ; pwd`
export BASEDIR=$H
. $H/bin/set-classpath.sh

ZK_USER="root"
ZK_PASSWORD="secret"
INSTANCE="cloudbase"
ZK_HOST="f-2-15.llgrid.ll.mit.edu"
TSERVER_HOST="f-2-13.llgrid.ll.mit.edu:9997"
TABLENAME="test_table4321"

echo "Testing to get total entries in the cloud via the Monitor."
set -x
java -classpath .:$CLASSPATH -Djava.util.logging.config.file=conf/logging.properties edu.mit.ll.d4m.db.cloud.TestMonitor $ZK_USER $ZK_PASSWORD $INSTANCE $ZK_HOST $TSERVER_HOST $TABLENAME




