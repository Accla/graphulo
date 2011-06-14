
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh


echo "----------------------------- Hadoop Home" "$HADOOP_HOME"
echo "----------------------------- Stop Hadoop"


$HADOOP_HOME/bin/stop-all.sh

