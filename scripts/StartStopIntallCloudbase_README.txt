This readme file explains the following;


1. Installing, starting, stopping, and removing hadoop, cloudbase, and zookeeper to
   multiple nodes (multiple instances) to local directories on a cluster.

2. Starting and stopping an existing hadoop, cloudbase, and zookeeper that is
   previously installed on a cluster that is configured for fully distributed mode.
   (one master instance with multiple slaves)




----------------------------------------------------------------------------------------
1. Installing, starting, stopping, and removing hadoop, cloudbase, and zookeeper to
   multiple nodes (multiple instances) to local directories on a cluster.

   a. cd to ThisApp/bin
   b. If not already installed, add the node names you want to install to
      the file "instance_hosts" and run ./cloudbase_instance_processor.sh install.
   c. To start run ./cloudbase_instance_processor.sh start.
      Note; Cloudbase needs to be init-ed by hand at the moment so the init
      process is commented out in the script "start_cb_hd_local.sh".
      Zookeeper and Hadoop are started only. After init- ing cloudbase
      you may uncomment ./start_cloudbase.sh in "start_cb_hd_local.sh"
      for ease in starting and stopping.    
   d. To Stop run ./cloudbase_instance_processor.sh stop.
   e. To remove run ./cloudbase_instance_processor.sh remove.

   Scripts;
   cloudbase_instance_processor.sh
   install_cb_hd_local.sh
   start_cb_hd_local.sh
   stop_cb_hd_local.sh
   remove_cb_hd_local.sh

----------------------------------------------------------------------------------------
2. Starting and stopping an existing hadoop, cloudbase, and zookeeper that is
   previously installed on a cluster that is configured for fully distribute mode.
   (one master instance with multiple slaves)

   a. cd to ThisApp/bin
   b. Make sure that HADOOP_HOME, CLOUDBASE_HOME, and ZOOKEEPER_HOME are set properly in setenv.sh.
   c. To start run ./start_all.sh.
   d. To Stop run ./stop_all.sh.

   Scripts;
   start_all.sh
   stop_all.sh
   setenv.sh
   start_cloudbase.sh
   stop_cloudbase.sh
   start_hadoop.sh
   stop_hadoop.sh
   start_zookeeper.sh
   stop_zookeeper.sh


