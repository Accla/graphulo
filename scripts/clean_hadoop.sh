
#!/bin/sh


PRGDIR=`dirname "$PRG"`

HOME=`cd "$PRGDIR/.." ; pwd`


. $HOME/bin/setenv.sh


echo "----------------------------- Hadoop HDFS" "$HADOOP_HDFS"
echo "----------------------------- Cleaning Hadoop"

sudo rm -R $HADOOP_HDFS/mapred
sudo rm -R $HADOOP_HDFS/dfs

rm -R $HADOOP_HOME/logs
mkdir $HADOOP_HOME/logs
