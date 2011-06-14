
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh

$CLOUDBASE_HOME/bin/stop-all.sh

sleep 5

$HADOOP_HOME/bin/stop-all.sh

sleep 5

$ZOOKEEPER_HOME/bin/zkServer.sh stop
