#!/bin/sh


#HADOOP_HDFS=/tmp/hadoop-wi20909
#CLOUDBASE_HOME=/home/wi20909/cloudbase/cloudbase-1.0
#ZOOKEEPER_HOME=/home/wi20909/zookeeper
#HADOOP_HOME=/home/wi20909/hadoopdev/hadoop-0.19.0
#HADOOP_HOSTNAME=wi20909-desktop
#HADOOP_USER=wi20909


HADOOP_HDFS=$HOME/temp/hadoop
CLOUDBASE_HOME=$HOME/temp/
ZOOKEEPER_HOME=$HOME/temp/zookeeper-3.3.1
HADOOP_HOME=$HOME/temp/hadoop-0.20.2
HADOOP_HOSTNAME=localhost
HADOOP_USER=cr18739
#JAVA_HOME=/home/cr18739/java


export CLOUDBASE_HOME
export ZOOKEEPER_HOME
#export JAVA_HOME
export HADOOP_HOME
export HADOOP_HOSTNAME
export HADOOP_USER



echo "----------------------------- Env Set "
