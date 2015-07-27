#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

# Install server-side iterators in Accumulo.
if [ -z ${ACCUMULO_HOME+x} ]; then
  echo "Not installing Graphulo JAR in Accumulo server because ACCUMULO_HOME is not set";
else
  cp target/graphulo-*.jar "$ACCUMULO_HOME/lib/ext"
fi

# Install client code + dependencies in D4M.
if [ -z ${D4M_HOME+x} ]; then
  echo "Not installing Graphulo JAR in D4M installation because D4M_HOME is not set";
else
  cp target/graphulo-*.jar "$D4M_HOME/lib"
  unzip -ouq target/graphulo-*-libext.zip -d "$D4M_HOME"
  # Replace DBinit.m in D4M matlab_src with new version.
  mv "$D4M_HOME/DBinit.m" "$D4M_HOME/matlab_src"
fi
