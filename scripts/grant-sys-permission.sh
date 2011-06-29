#!/bin/sh

#Grant system permission in the cloudbase instance
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

if [ $# -lt 6 ]
then
   echo "This script is used to grant systems permission to the user."
   echo "Usage: $PRG <root> <root password> <hostname> <cloudbase instance> <new username> <permission - CREATE_TABLE, DROP_TABLE, ALTER_TABLE> "
   echo "  Permission can be either CREATE_TABLE, DROP_TABLE, or ALTER_TABLE."
   echo ""
   echo "Example -  grant-sys-permission.sh   root RootPassword f-2-6.llgrid.ll.mit.edu cloudbase JoeUser CREATE_TABLE"

   exit 1
fi

_ROOT=$1
_ROOT_PASSWD=$2
## Cloudbase host
_HOST=$3
_INSTANCE=$4
_USER=$5
_PERMISSION=$6



set -x
java -classpath .:$CLASSPATH edu.mit.ll.d4m.db.cloud.util.GrantSystemPermission ${_ROOT} ${_ROOT_PASSWD} ${_HOST} ${_INSTANCE} ${_USER} ${_PERMISSION}
echo "----------------------------- Job complete"
#-Xms2000m
