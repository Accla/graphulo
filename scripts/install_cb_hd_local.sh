
#!/bin/sh


PRGDIR=`dirname "$PRG"`

H=`cd "$PRGDIR/.." ; pwd`

node_name=$1
base_dir=$2
counter=$3

. $base_dir/bin/setenv.sh


echo " CLOUDBASE_HOME = $CLOUDBASE_HOME"
echo " getting node name = $node_name"
#touch /state/partition1/crossmounts/$node_name
cp -R $base_dir/ForNodeDist/cloudbase /state/partition1/crossmounts/$node_name/hadoop
cp -R $base_dir/ForNodeDist/hadoop /state/partition1/crossmounts/$node_name/hadoop
cp -R $base_dir/ForNodeDist/zookeeper /state/partition1/crossmounts/$node_name/hadoop


### hadoop ports

num=9000
num_a=9100
num1=50100
num2=50200
num3=50300
num4=50400
num5=50500
num6=50600
num7=50700

num8=60000
num9=60100

## zookeeper ports
num10=2180

num=$(($num + $counter))
num_a=$(($num_a + $counter))

num1=$(($num1 + $counter))
num2=$(($num2 + $counter))
num3=$(($num3 + $counter))
num4=$(($num4 + $counter)) 
num5=$(($num5 + $counter)) 
num6=$(($num6 + $counter))
num7=$(($num7 + $counter))

num8=$(($num8 + $counter)) 
num9=$(($num9 + $counter))
num10=$(($num10 + $counter))   

## cloudbase ports
#9997
#9998
#9999
#11223
#50096
#9996
#50095

num11=9820
num12=9840
num13=9990
num14=11223
num15=50800
num16=9880
num17=50900


num11=$(($num11 + $counter))
num12=$(($num12 + $counter))
num13=$(($num13 + $counter))
num14=$(($num14 + $counter)) 
num15=$(($num15 + $counter)) 
num16=$(($num16 + $counter))
num17=$(($num17 + $counter))



############### MODIFY CLOUDBASE CONFIGS
CLOUDBASE_CONFIG=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/cloudbase-site.xml
CLOUDBASE_CONFIG_TEMP=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/cloudbase-site.tmp

sed s/f-2-1/$node_name/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG
sed s/2181/$num10/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG

sed s/cloudbase_hdfs/cloudbase_hdfs2_$node_name/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG

sed s/9997/$num11/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG
sed s/9998/$num12/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG
sed s/9999/9999/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG
sed s/11223/$num14/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG
sed s/50096/$num15/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG
sed s/9996/$num16/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG
sed s/50095/$num17/g $CLOUDBASE_CONFIG > $CLOUDBASE_CONFIG_TEMP && mv $CLOUDBASE_CONFIG_TEMP $CLOUDBASE_CONFIG

CLOUDBASE_MASTER=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/masters
CLOUDBASE_MASTER_TEMP=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/masters.tmp
sed s/f-2-1/$node_name/g $CLOUDBASE_MASTER > $CLOUDBASE_MASTER_TEMP && mv $CLOUDBASE_MASTER_TEMP $CLOUDBASE_MASTER

CLOUDBASE_SLAVE=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/slaves
CLOUDBASE_SLAVE_TEMP=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/slaves.tmp
sed s/f-2-1/$node_name/g $CLOUDBASE_SLAVE > $CLOUDBASE_SLAVE_TEMP && mv $CLOUDBASE_SLAVE_TEMP $CLOUDBASE_SLAVE

CLOUDBASE_ENV=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/cloudbase-env.sh
CLOUDBASE_ENV_TEMP=/state/partition1/crossmounts/$node_name/hadoop/cloudbase/conf/cloudbase-env.tmp
sed s/f-2-1/$node_name/g $CLOUDBASE_ENV > $CLOUDBASE_ENV_TEMP && mv $CLOUDBASE_ENV_TEMP $CLOUDBASE_ENV




############### MODIFY HADOOP CONFIGS
HADOOP_CONFIG=/state/partition1/crossmounts/$node_name/hadoop/hadoop/conf/hadoop-site.xml
HADOOP_CONFIG_TEMP=/state/partition1/crossmounts/$node_name/hadoop/hadoop/conf/hadoop-site.tmp
sed s/f-2-1/$node_name/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG

sed s/9000/$num/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG
sed s/9001/$num_a/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG


sed s/50010/$num1/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG
sed s/50075/$num2/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG
sed s/50020/$num3/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG
sed s/50070/$num4/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG
sed s/50475/$num5/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG
sed s/50470/$num6/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG
sed s/50090/$num7/g $HADOOP_CONFIG > $HADOOP_CONFIG_TEMP && mv $HADOOP_CONFIG_TEMP $HADOOP_CONFIG


HADOOP_MASTER=/state/partition1/crossmounts/$node_name/hadoop/hadoop/conf/masters
HADOOP_MASTER_TEMP=/state/partition1/crossmounts/$node_name/hadoop/hadoop/conf/masters.tmp
sed s/f-2-1/$node_name/g $HADOOP_MASTER > $HADOOP_MASTER_TEMP && mv $HADOOP_MASTER_TEMP $HADOOP_MASTER

HADOOP_SLAVE=/state/partition1/crossmounts/$node_name/hadoop/hadoop/conf/slaves
HADOOP_SLAVE_TEMP=/state/partition1/crossmounts/$node_name/hadoop/hadoop/conf/slaves.tmp
sed s/f-2-1/$node_name/g $HADOOP_SLAVE > $HADOOP_SLAVE_TEMP && mv $HADOOP_SLAVE_TEMP $HADOOP_SLAVE


############### MODIFY ZOOKEEPER CONFIGS 2181
ZOOKEEPER_CONFIG=/state/partition1/crossmounts/$node_name/hadoop/zookeeper/conf/zoo.cfg
ZOOKEEPER_CONFIG_TEMP=/state/partition1/crossmounts/$node_name/hadoop/zookeeper/conf/zoo.tmp
sed s/2181/$num10/g $ZOOKEEPER_CONFIG > $ZOOKEEPER_CONFIG_TEMP && mv $ZOOKEEPER_CONFIG_TEMP $ZOOKEEPER_CONFIG
sed s/f-2-1/$node_name/g $ZOOKEEPER_CONFIG > $ZOOKEEPER_CONFIG_TEMP && mv $ZOOKEEPER_CONFIG_TEMP $ZOOKEEPER_CONFIG



#rm -r /state/partition1/crossmounts/$node_name/hadoop/hadoop
#rm -r /state/partition1/crossmounts/$node_name/hadoop/cloudbase

