#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

# Install server-side iterators in Accumulo.
cp graphulo/target/graphulo-*.jar $ACCUMULO_HOME/lib/ext

# Install client code + dependencies in D4M.
cp graphulo/target/graphulo-*.jar $D4M_HOME/lib
unzip -ouq graphulo/target/libext*.zip -d $D4M_HOME
# Replace DBinit.m in D4M matlab_src with new version.
mv $D4M_HOME/DBinit.m $D4M_HOME/matlab_src
