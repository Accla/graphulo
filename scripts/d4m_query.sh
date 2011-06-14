#!/bin/sh

PRG=$0
PRGDIR=`dirname "$PRG"`
H=`cd "$PRGDIR/.." ; pwd`
. $H/bin/setenv.sh

export BASEDIR=$H
echo "base dir = $BASEDIR"
. $H/bin/set-classpath.sh

host="f-2-10.llgrid.ll.mit.edu"
tableName="ReutersData"
limit=1000
FILE1=:
FILE2=:

if [ $# -eq 5 ]
then 
  host=$1
  tableName=$2
  row=$3
  col=$4
  limit=$5
fi
FILE1=$row
FILE2=$col
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

#FILE1=1,2,3,4,5,6,7,8,9,10,
#FILE2=1,22,333,4444,55555,666666,7777777,88888888,999999999,10101010101010101010,

#FILE2=:

#FILE1=1,2,3,8,9,10,
#FILE2=1,22,333,88888888,999999999,7777777,



#FILE1=:
#FILE2=:
#FILE2=333,

echo "classpath = $CLASSPATH"

echo "----------------------------- Loading D4mDbQuery"
#./hadoop jar $D4M_HOME/build/d4m_api-2.0.jar edu.mit.ll.d4m.db.cloud.D4mDbQuery localhost test_table9 $FILE1 $FILE2
java -classpath .:$CLASSPATH edu.mit.ll.d4m.db.cloud.D4mDbQuery ${host} ${tableName} $FILE1 $FILE2 ${limit}
echo "----------------------------- Job complete"
#-Xms2000m
