d4m_api_java
============

D4M_API is set of D4M scripts that enable 
the storage and retrieval of associative arrays into a database.

### Requirements
* Matlab (or GNU Octave 3.2.2+ with Java package 1.2.6+)
* Accumulo 1.6.0+



### Building
[![Build Status](https://api.shippable.com/projects/54f27f245ab6cc13528fd44d/badge?branchName=master)](https://app.shippable.com/projects/54f27f245ab6cc13528fd44d/builds/latest)
[![Build Status](https://travis-ci.org/Accla/d4m_api_java.svg)](https://travis-ci.org/Accla/d4m_api_java)

The maven script will build everything completely on Unix-like systems.
On Windows systems, DBinit.m may not be built. See the message in the build after running `mvn package`.

`mvn package -DskipTests=true` to compile and build JARs, skipping tests.

### Testing
Java tests are mostly broken.

<!--
Tests only run on Unix-like systems.

* `mvn test` to run tests on [MiniAccumulo][], 
a lightweight Accumulo instance started before and stopped after each test class.
* `mvn test -DTEST_CONFIG=local` to run tests on a local instance of Accumulo.
See TEST_CONFIG.java for changing connection parameters, such as testing on a remote Accumulo instance.
* `post-test.bash` is a utility script to output test results to the console.
Test results are saved in the `shippable/testresults` folder.

[MiniAccumulo]: https://accumulo.apache.org/1.6/accumulo_user_manual.html#_mini_accumulo_cluster
-->


### Using
To add to a D4M installation:

1. Copy `target/d4m_api_java-VERSION.jar` into `d4m_api/lib`.
2. Extract target/libext-VERSION.zip into `d4m_api`.
3. Move `d4m_api/DBinit.m` into `d4m_api/matlab_src`.

Add the following to your ~/matlab/startup.m file (or ~/.octaverc file).

      addpath('<ParentDir>/d4m_api/matlab_src');  % Replace <ParentDir> with location of d4m_api.
      DBinit;    % Initalizes java path.  NOTE: Octave requires Java package be installed.

### Testing in Matlab D4M 

* Edit `d4m_api/matlab_src/TEST/DBsetup.m` to use connection information for your Accumulo instance.
* Start Matlab (or octave --traditional).
* cd to d4m_api/matlab_src/TEST, and run any script ending in TEST.m
* To run all tests, type `Atest = runTESTdir('./')`
