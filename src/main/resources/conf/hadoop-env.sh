# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/j2sdk1.5-sun
export JAVA_HOME=/home/wi20909/java/jdk1.6.0_11

# Extra Java CLASSPATH elements.  Optional.
# export HADOOP_CLASSPATH=

export ACCUMULO_HOME=/home/wi20909/accumulo/accumulo
export HBASE_HOME=/home/wi20909/hadoopdev/hbase-0.19.0
export HJOBS_HOME=/home/wi20909/hadoopdev/accumulo_tool_api/build/accumulo_tool_api-1.0


#$HBASE_HOME/conf:$HBASE_HOME/hbase-0.19.0.jar:$HBASE_HOME/hbase-0.19.0-test.jar:$HJOBS_HOME/build/hjobs-0.19.1-dev.jar

export HADOOP_CLASSPATH=$ACCUMULO_HOME/conf:$HJOBS_HOME/accumulo_tool_api-1.0.jar:$ACCUMULO_HOME/lib/accumulo.jar:$HJOBS_HOME/lib/lucene-core-2.4.1.jar:$HJOBS_HOME/lib/lucene-demos-2.4.1.jar:$HJOBS_HOME/lib/poi-3.2-FINAL-20081019.jar:$HJOBS_HOME/lib/poi-contrib-3.2-FINAL-20081019.jar:$HJOBS_HOME/lib/poi-scratchpad-3.2-FINAL-20081019.jar


# The maximum amount of heap to use, in MB. Default is 1000.
# export HADOOP_HEAPSIZE=2000

# Extra Java runtime options.  Empty by default.
# export HADOOP_OPTS=-server

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
# export HADOOP_TASKTRACKER_OPTS=
# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
# export HADOOP_CLIENT_OPTS

# Extra ssh options.  Empty by default.
# export HADOOP_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HADOOP_CONF_DIR"

# Where log files are stored.  $HADOOP_HOME/logs by default.
# export HADOOP_LOG_DIR=${HADOOP_HOME}/logs

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export HADOOP_SLAVES=${HADOOP_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export HADOOP_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HADOOP_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export HADOOP_PID_DIR=/var/hadoop/pids

# A string representing this instance of hadoop. $USER by default.
# export HADOOP_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HADOOP_NICENESS=10
