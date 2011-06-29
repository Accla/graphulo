#!/bin/sh

#Grant table permission for the user
PRG=$0
PRGDIR=`dirname "$PRG"`
H=`cd "$PRGDIR/.." ; pwd`
. $H/bin/setenv.sh


export BASEDIR=$H
echo "base dir = $HOME"
. $H/bin/set-classpath.sh


echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""


#echo "----------------------------- java_home=$JAVA_HOME"
#echo "----------------------------- HADOOP_HOME=$HADOOP_HOME"
#echo "----------------------------- HJOBS_HOME=$HJOBS_HOME"
#echo "----------------------------- HADOOP_USER=$HADOOP_USER"
#echo "----------------------------- HADOOP_HOSTNAME=$HADOOP_HOSTNAME"

#cd "$HADOOP_HOME"/bin

if [ $# -lt 5 ]
then
   echo "Usage; $PRG <root> <root password> <hostname> <cloudbase instance> <tablename> "
   echo "Example -  createtable.sh root blah f-2-6.llgrid.ll.mit.edu cloudbase tableFOO  "
   exit 1
fi

_ROOT=$1
_ROOT_PASSWD=$2
## Cloudbase host
_HOST=$3
_INSTANCE=$4

_TABLENAME=$5




set -x
java -classpath .:$CLASSPATH edu.mit.ll.d4m.db.cloud.util.CreateTable ${_ROOT} ${_ROOT_PASSWD} ${_HOST} ${_INSTANCE} ${_TABLENAME}
echo "----------------------------- Job complete"
#-Xms2000m
