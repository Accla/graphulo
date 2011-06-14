
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh


$CLOUDBASE_HOME/bin/cloudbase.sh init

sleep 5

$CLOUDBASE_HOME/bin/start-all.sh
