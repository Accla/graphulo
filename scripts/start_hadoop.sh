
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh


echo "----------------------------- Hadoop Home" "$HADOOP_HOME"

echo "----------------------------- Format Hadoop namenode"

$HADOOP_HOME/bin/hadoop namenode -format
sleep 5
echo "----------------------------- Start Hadoop"
$HADOOP_HOME/bin/start-all.sh

