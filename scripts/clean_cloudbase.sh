
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh



sudo rm -R $CLOUDBASE_HOME/logs
mkdir $CLOUDBASE_HOME/logs
chmod 777 $CLOUDBASE_HOME/logs
