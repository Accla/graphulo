
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


node_name=$1
base_dir=$2

#. $H/hadoop/hadoopdev/cloud-sks-tools-1.0/bin/setenv.sh
#. $H/bin/setenv.sh



echo " getting node name = $node_name"
#touch /state/partition1/crossmounts/$node_name
#cp -R $CLOUDBASE_HOME /state/partition1/crossmounts/$node_name/hadoop
#cp -R $HADOOP_HOME /state/partition1/crossmounts/$node_name/hadoop



CLOUDBASE_BIN=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/bin
HADOOP_BIN=/state/partition1/crossmounts/$node_name/hadoop/hadoop/bin
ZOOKEEPER_BIN=/state/partition1/crossmounts/$node_name/hadoop/zookeeper/bin

export CLOUDBASE_HOME=/state/partition1/crossmounts/$node_name/hadoop/cloudbase
export HADOOP_HOME=/state/partition1/crossmounts/$node_name/hadoop/hadoop
export ZOOKEEPER_HOME=/state/partition1/crossmounts/$node_name/hadoop/zookeeper


#$H/bin/start_zookeeper.sh
#
#$H/bin/start_zookeeper.sh

sleep 10
cd $CLOUDBASE_BIN
sleep 10

#$H/bin/clean_cloudbase.sh

sleep 10
#./stop-all.sh


sleep 20
#$H/bin/clean_hadoop.sh
#$H/bin/clean_nodes.sh
cd $HADOOP_BIN

./stop-all.sh

sleep 10

cd $ZOOKEEPER_BIN

./zkServer.sh stop




cd $H


