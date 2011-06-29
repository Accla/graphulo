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

if [ $# -lt 7 ]
then
   echo "Usage: $PRG <root> <root password> <hostname> <cloudbase instance> <username> <tablename> <permission - READ,WRITE,BULK_IMPORT> "
   echo "  Permission can be either READ, WRITE, or BULK_IMPORT."
   echo ""
   echo "Example -  grant-tab-permission.sh   root RootPassword f-2-6.llgrid.ll.mit.edu cloudbase JoeUser TableBoo READ"

   exit 1
fi

_ROOT=$1
_ROOT_PASSWD=$2
## Cloudbase host
_HOST=$3
_INSTANCE=$4
_USER=$5
_TABLENAME=$6
_PERMISSION=$7



set -x
java -classpath .:$CLASSPATH edu.mit.ll.d4m.db.cloud.util.GrantTablePermissions ${_ROOT} ${_ROOT_PASSWD} ${_HOST} ${_INSTANCE} ${_USER} ${_TABLENAME} ${_PERMISSION}
echo "----------------------------- Job complete"
#-Xms2000m
