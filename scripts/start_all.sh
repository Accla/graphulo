
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`


. $H/bin/setenv.sh

$H/bin/start_zookeeper.sh

sleep 10


$H/bin/clean_hadoop.sh
#$H/bin/clean_nodes.sh

sleep 5

$H/bin/start_hadoop.sh

sleep 10

$H/bin/clean_cloudbase.sh


sleep 50

$H/bin/start_cloudbase.sh

sleep 5

