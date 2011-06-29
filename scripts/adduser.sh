#!/bin/sh

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
   echo "Usage; $PRG <root> <root password> <hostname> <cloudbase instance> <new username> <new user password> [authorizations] "
   echo "  'Authorizations' is optional and is a comma-delimited string."
   echo "Example -  adduser.sh   root blah f-2-6.llgrid.ll.mit.edu cloudbase JoeUser JoePassword \"secret,fouo\""
   exit 1
fi

_ROOT=$1
_ROOT_PASSWD=$2
## Cloudbase host
_HOST=$3
_INSTANCE=$4
_USER=$5
_USER_PASSWD=$6

#Authorizations should be a comma-separated list
if [ $# -gt 6 ]
then
_AUTHS=$7
fi

#cd $D4M_HOME/build/classes


set -x
java -classpath .:$CLASSPATH edu.mit.ll.d4m.db.cloud.util.AddUser ${_ROOT} ${_ROOT_PASSWD} ${_HOST} ${_INSTANCE} ${_USER} ${_USER_PASSWD} ${_AUTHS}
echo "----------------------------- Job complete"
#-Xms2000m
