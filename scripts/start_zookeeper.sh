
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh


echo "----------------------------- ZOOKEEPER Home" "$ZOOKEEPER_HOME"



echo "----------------------------- Starting Zookeeper"
nohup $ZOOKEEPER_HOME/bin/zkServer.sh start &

