
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh


echo "----------------------------- ZOOKEEPER Home" "$ZOOKEEPER_HOME"



echo "----------------------------- Stopping Zookeeper"
$ZOOKEEPER_HOME/bin/zkServer.sh stop

