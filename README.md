d4m_api_java
============

NOTE: This is the Accumulo 1.6.0+ version.
*This build will not work against Accumulo 1.5 and previous.*

[![Build Status](https://api.shippable.com/projects/54f27f245ab6cc13528fd44d/badge?branchName=master)](https://app.shippable.com/projects/54f27f245ab6cc13528fd44d/builds/latest)
[![Build Status](https://travis-ci.org/Accla/d4m_api_java.svg)](https://travis-ci.org/Accla/d4m_api_java)

### Using
To add to a D4M installation:

1. Copy `target/d4m_api_java-VERSION.jar` into `d4m_api/lib`.
2. Extract target/libext-VERSION.zip into `d4m_api`.
3. Move `d4m_api/DBinit.m` into `d4m_api/matlab_src`.

### Building
The maven script will build everything completely on linux.
On non-linux systems, DBinit.m may not be built. See the message in the build after running `mvn package`.

`mvn package -DskipTests=true` to compile and build JARs.

### Testing
* `mvn test` to run tests on [MiniAccumulo][]
* `mvn test -DTEST_CONFIG=local` to run tests on a local instance of Accumulo.
See TEST_CONFIG.java for defining other options.
* `post-test.bash` is a utility script to output test results to the console.

[MiniAccumulo]: https://accumulo.apache.org/1.6/accumulo_user_manual.html#_mini_accumulo_cluster

## Original Readme
<pre>
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% Architect: Dr. Jeremy Kepner (kepner@ll.mit.edu)
% Software Engineers: 
%   Mr. Craig McNally (cmcnally@ll.mit.edu)
%   Dr. Jeremy Kepner (kepner@ll.mit.edu)
%   Mr. Will Smith (will.smith@ll.mit.edu)
%   Mr. Chuck Yee (yee@ll.mit.edu)
%
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

INTRODUCTION

  D4M_API is set of D4M scripts that enable the storage
  and retrieval of associative arrays into a database.

REQUIREMENTS

  D4M (standalone)
  -Matlab (or GNU ctave 3.2.2+ with Java package 1.2.6+)

  D4M (w/database)
    -Accumulo
      -Zookeeper
      -Hadoop File system
      -Accumulo database
    -HBASE (TBD)
    -OTHERS (TBD)



INSTALLING AND RUNNING FULL OR LLONLY DISTRIBUTION:

  - Add the following to your ~/matlab/startup.m file (or ~/.octaverc file).

      addpath('<ParentDir>/d4m_api/matlab_src');  % Replace <ParentDir> with location of d4m_api.
      DBinit;    % Initalizes java path.  NOTE: Octave requires Java package be installed.

  - Edit matlab_src/DBinit.m file and find missing .jar files and put them in lib directory.

       The ExternalContrib file also contains brief description of the jars.

  - Edit d4m_api/matlab_src/TEST/DBsetup.m so it points to your Zookeeper server.

  - Start Matlab (or octave --traditional).

  - cd to d4m_api/matlab_src/TEST, and run any script ending in TEST.m

  - To run all tests type:

       Atest = runTESTdir('./')


INSTALLING LLONLY STUB DISTRIBUTION:

  - Assumes a LLONLY distribution has already been installed.

  - Unpack LLONLY STUB distribution on top of existing directory
    to get most recent changes to LLONLY distribution.


RUNNING ON MacOSX

  Same as above.

CREATING A FULL DISTRIBUTION:
  svn export <URL>
  ant package
  ant zip

CREATING A LLONLY DISTRIBUTION:
  ant ll_package
  ant zip


CREATING A LLONLY STUB DISTRIBUTION:

KNOWN ISSUES

  GNU Octave 3.2.2 / Java PKG 1.2.6
    matlab_src/runTESTdir only works if Octave is launched in TEST directory

</pre>
