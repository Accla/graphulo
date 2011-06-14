
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh




$CLOUDBASE_HOME/bin/stop-all.sh
