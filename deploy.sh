#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

# Install server-side iterators in Accumulo.
if [ -z ${ACCUMULO_HOME+x} ]; then
  echo "Not installing Graphulo JAR in Accumulo server because ACCUMULO_HOME is not set";
else
  cp target/graphulo-*-alldeps.jar "$ACCUMULO_HOME/lib/ext"
  echo "Installed Graphulo alldeps jar into ACCUMULO_HOME at $ACCUMULO_HOME"
fi

# Install client code + dependencies in D4M.
if [ -z ${D4M_HOME+x} ]; then
  echo "Not installing Graphulo JAR in D4M installation because D4M_HOME is not set";
else
  cp $(echo target/graphulo-*.jar | tr ' ' '\n' | grep -v alldeps) "$D4M_HOME/lib"
  cp -f target/graphulo-*-alldeps.jar $D4M_HOME/lib
  cp -f target/graphulo-*-libext.zip $D4M_HOME
  unzip -ouq target/graphulo-*-libext.zip -d "$D4M_HOME"
  # Replace DBinit.m in D4M matlab_src with new version.
  mv "$D4M_HOME/DBinit.m" "$D4M_HOME/matlab_src"
  echo "Installed Graphulo into D4M_HOME at $D4M_HOME"
fi
