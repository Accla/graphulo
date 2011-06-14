
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`

node_name=$1
base_dir=$2

#. $H/hadoop/hadoopdev/cloud-sks-tools-1.0/bin/setenv.sh
#. $H/bin/setenv.sh

JAVA_BIN=/home/gridsan/hadoop/java/jdk1.6.0_11/bin
JAVA_HOME=/home/gridsan/hadoop/java/jdk1.6.0_11

PATH=$PATH:$JAVA_BIN
export PATH


echo " getting node name = $node_name"
#touch /state/partition1/crossmounts/$node_name
#cp -R $CLOUDBASE_HOME /state/partition1/crossmounts/$node_name/hadoop
#cp -R $HADOOP_HOME /state/partition1/crossmounts/$node_name/hadoop

export CLOUDBASE_HOME=/state/partition1/crossmounts/$node_name/hadoop/cloudbase
export HADOOP_HOME=/state/partition1/crossmounts/$node_name/hadoop/hadoop
export ZOOKEEPER_HOME=/state/partition1/crossmounts/$node_name/hadoop/zookeeper


CLOUDBASE_BIN=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/bin
HADOOP_BIN=/state/partition1/crossmounts/$node_name/hadoop/hadoop/bin
ZOOKEEPER_BIN=/state/partition1/crossmounts/$node_name/hadoop/zookeeper/bin


#$H/bin/start_zookeeper.sh
#

cd $ZOOKEEPER_BIN

nohup ./zkServer.sh start &

sleep 20

#$H/bin/clean_hadoop.sh
#$H/bin/clean_nodes.sh

cd $HADOOP_BIN
./hadoop namenode -format

sleep 10

./start-all.sh

cd $CLOUDBASE_BIN
sleep 55

#./cloudbase.sh init
#$H/bin/clean_cloudbase.sh


sleep 40

#./start-all.sh


cd $H


