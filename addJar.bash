#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail
cp target/d4m_api_java*.jar $ACCUMULO_HOME/lib/ext
cp target/d4m_api_java*.jar $D4M_HOME/lib
unzip -ouq target/libext*.zip -d $D4M_HOME
mv $D4M_HOME/DBinit.m $D4M_HOME/matlab_src
cp src/main/resources/log4j.xml $D4M_HOME/lib
