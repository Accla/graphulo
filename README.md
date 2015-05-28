Looking for vanilla d4m_api_java without Graphulo?  Go to the subdirectory `d4m_api_java/`.

Graphulo
========

Graphulo is a Java library that provides graph primitives and algorithms
that run server-side on the Apache [Accumulo][] database.

Graph primitives loosely follow the [GraphBLAS][] spec.
Graph algorithms include Breadth First Search, finding a k-Truss subgraph, 
computing Jaccard coefficients, and performing non-negative matrix factorization. 

Our use case is *queued* analytics that run on a table subset,
as opposed to *whole table* analytics that are better off using a filesystem than a database.
We therefore prioritize low latency over high throughput when tradeoffs are necessary.

Graphulo's design resembles that of a stored procedure in classic relational databases.
The client calls a graph primitive that creates a new table 
in the database rather than gathering results at the client.
Our core approach is performing a scan with iterators that allow reading from multiple tables
and writing to multiple tables, as opposed to ordinary scans 
that read from a single table and send results back to the client.

Graphulo is tested on Accumulo 1.6.0 and 1.6.1. 

[Accumulo]: https://accumulo.apache.org/
[GraphBLAS]: http://istc-bigdata.org/GraphBlas/


### Directory Structure

An asterisk indicates files/folders recommended to change, or not made yet.

<pre>
graphulo/               d4m_api_java project directory
  src/                                 
    assembly/...        Files for building graphulo.
    main/               Main code and resources. Included in output JAR.
      java/...          
      resources/        Contents copied into output JAR.
        log4j.xml       Logging configuration for clients at runtime.
    test/               Test code and resources. Not included in output JAR.
      java/...             
      resources/
        log4j.xml       Logging configuration for tests.
        data/...        Data for testing graphulo. 
  target/
    JAR
    LIBEXT ZIP
  pom.xml               Maven Project Object Model for graphulo.
  post-test.bash        Script to display output of tests from shippable/testresults.

d4m_api_java/           d4m_api_java project directory
  src/...               Similar to graphulo/src/...
  target/...            Similar to graphulo/target/...
  pom.xml               Maven Project Object Model for d4m_api_java.
  post-test.bash        Script to display output of tests from shippable/testresults.

distribution/           Where the distribution build for graphulo occurs.
  src/assembly/...      Similar to graphulo/src/assembly/...
  target/
    graphulo-...-bin.jar   Binaries for graphulo and d4m_api_java. Put this jar in a client code's classpath.
    graphulo-...-dist.zip  Distribution zip file, containing all source and binaries.
  pom.xml               Maven Project Object Model for creating the distribution build.

deploy.sh               Script to deploy a graphulo build to Accumulo and Matlab D4M.
pom.xml                 Maven Project Object Model for parent of d4m_api_java and graphulo.
README.md               This file.
                        (-Below files only in git repository-)
.gitignore              Files and folders to exclude from git.
.travis.yml             Enables continuous integration testing.
shippable.yml           Enables continuous integration testing.
</pre>

[Project Object Model]: https://maven.apache.org/guides/introduction/introduction-to-the-pom.html

### Building
[![Build Status](https://api.shippable.com/projects/54f27f245ab6cc13528fd44d/badge?branchName=master)](https://app.shippable.com/projects/54f27f245ab6cc13528fd44d/builds/latest)
[![Build Status](https://travis-ci.org/Accla/d4m_api_java.svg)](https://travis-ci.org/Accla/d4m_api_java)

Prerequisite: Install [Maven](https://maven.apache.org/download.cgi).

Run `mvn package -DskipTests=true` to compile and build d4m_api_java and Graphulo.
The main distribution files are a JAR containing d4m_api_java and Graphulo code, 
and a libext ZIP file containing dependencies for both projects.
The JAR and ZIP are created inside the `graphulo/target/` directory.

The maven script will build everything on Unix-like systems.
On Windows systems, `DBinit.m` may not be built (used in D4M installation). 
See the message in the build after running `mvn package`.

### Testing
Tests only run on Unix-like systems.

* `mvn test` to run tests on [MiniAccumulo][], 
a portable, lightweight Accumulo instance started before and stopped after each test class.
* `mvn test -DTEST_CONFIG=local` to run tests on a local instance of Accumulo.
See TEST_CONFIG.java for changing connection parameters, such as testing on a remote Accumulo instance.
* `post-test.bash` is a utility script to output test results to the console.
Test results are saved in the `shippable/testresults` folder.
Run `mvn clean` to delete output from previously run tests.

[MiniAccumulo]: https://accumulo.apache.org/1.6/accumulo_user_manual.html#_mini_accumulo_cluster

### Examples
The classes in `graphulo/src/test/java/edu/mit/ll/graphulo/examples/`
contain simple, well-commented examples of how to use Graphulo.

* `mvn test -Dtest=TableMultExample` to insert a SCALE 10 graph into MiniAccumulo,
store the result of multiplying it with itself, and count the number of resulting entries.
* `mvn test -Dtest=AdjBFSExample` to insert a SCALE 10 graph into MiniAccumulo,
create a new table with the union sum of three steps of Breadth First Search, 
and count the number of resulting entries.
* View example output with `graphulo/post-test.bash`.

### Deploying
Execute `./deploy.sh`. This script will do the following:

1. ACCUMULO DEPLOY: Copy the Graphulo JAR into `$ACCUMULO_HOME/lib/ext`, the external library folder of your Accumulo installation,
so that Accumulo can use Graphulo's server-side iterators.
2. D4M DEPLOY: Copy the Graphulo JAR into `$D4M_HOME/lib`, unpack dependencies into `$D4M_HOME/libext`
and update `$D4M_HOME/matlab_src/DBinit.m`.

Feel free to delete or edit parts of the script for deploying to your environment.

### How to use Graphulo in Java client code
Include Graphulo's JAR in the Java classpath when running client code.  

The following code snippet is a good starting point for using Graphulo:

```java
// setup
Instance instance = new ZooKeeperInstance(INSTANCE_NAME, INSTANCE_ZK_HOST);
Connector connector = instance.getConnector(USERNAME, PASSWORD_TOKEN);
Graphulo graphulo = new Graphulo(connector, PASSWORD_TOKEN);

// call Graphulo functions...
graphulo.AdjBFS("Atable", v0, 3, "Rtable", null, "ADegtable", "deg", false, 5, 15);
```

See Examples above for more elaborate client code usage.

### How to use Graphulo in Matlab client code with D4M
The following code snippet is a good starting point for using Graphulo,
assuming the D4M libraries are also installed:

```Matlab
G = DBaddJavaOps('edu.mit.ll.graphulo.MatlabGraphulo','instance','localhost:2181','root','secret');
res = G.AdjBFS('Atable','v0;v7;v9;',3,'Rtable','','ADegtable','OutDeg',false,5,15);
```

## Implementation

### GraphBLAS mapping
* SpGEMM uses TwoTableIterator connected to a RemoteSourceIterator on table AT and a local iterator on table B.
TwoTableIterator configured with ROW_CARTESIAN_PRODUCT and emitNoMatchEntries=false.
* SpEWiseX uses TwoTableIterator connected to a RemoteSourceIterator on table A and a local iterator on table B.
TwoTableIterator configured with ROW_COLF_COLQ_MATCH and emitNoMatchEntries=false.
* SpEWiseSum uses TwoTableIterator connected to a RemoteSourceIterator on table A and a local iterator on table B.
TwoTableIterator configured with no multiplication and emitNoMatchEntries=true.
PreSumCacheIterator is important for efficiency.
* Sparse -- insert from client to table.
* Find -- scan from table to table.
* SpRef -- use RemoteWriteIterator with rowRanges and colFilter to output results to another table.
* SpAsgn -- unimplemented.
* Apply -- use an iterator with the function to apply + RemoteWriteIterator.
* Reduce -- use an iterator that does the reduction (say, count the number of columns in a row) 
and either send to client or to a RemoteWriteIterator.


### Iterators

##### RemoteSourceIterator
* `rowRanges` Row ranges to fetch from remote Accumulo table, Matlab syntax. (default ":" all) 
* `colFilter` String representation of column qualifiers, e.g. "a,b,c," (default "" = no filter) (no ranges allowed) 
Future: allow ranges and [Filter](https://accumulo.apache.org/1.6/apidocs/org/apache/accumulo/core/iterators/Filter.html) them
* `zookeeperHost` Address and port, e.g. "localhost:2181". Future: extract from Accumulo config if not provided
* `timeout` Zookeeper timeout between 1000 and 300000 (default 1000). Future: extract from Accumulo config if not provided
* `instanceName`
* `tableName`
* `username`
* `password` Anyone who can read the Accumulo table config or log files will see the password in plaintext.
* `iter.7` Class name of an iterator to place on the remote table scan, running on the remote Accumulo server at the specified priority. 
Run as many as desired, each with its own priority.
* `iter.7.type` e.g. "STRING". An option supplied to the LongCombiner iterator.

##### TwoTableIterator
* `B. ... ` All the options of RemoteSourceIterator, to read table A from a remote Accumulo table. 
Don't specify when operating on a single table.
* `(A/B).emitNoMatchEntries` Both false for multiply (intersection of entries); both true for sum (union of entries)
* `dot` Either "ROW_CARTESIAN_PRODUCT" or "ROW_COLF_COLQ_MATCH" or nothing.
* `multiplyOp` Name of class that implements IMultiplyOp. 

##### Future: PreSumCacheIterator
* `combiner` Name of class for "pre-summing" entries.
* `size` in bytes or entries?

##### RemoteWriteIterator
* `updater` Used to "collect" something to send to the client. Name of class that implements `KVUpdater` interface. 
The final state of the updater is sent to the client once the scan finishes,
or when at a checkpoint. (Updater must be capable of being sent in parts to the client in this case.)
* `checkpointNumEntries` Assume safe time to checkpoint is at the end of a row. Agh-- how to know the end of a row? 
Okay--- sacrifice minor, minor, minor performance for switching at the beginning of the next row.
* `checkpointTime` (in milliseconds) More useful than NumEntries.
* `tableName`
* `tableNameTranspose`




##### Other places to use iterators
* Can place an iterator before a TwoTableIterator (meaning lower priority), which runs on data from the local table 
before passing to the TwoTableIterator. Useful:
  * `SmallLargeRowFilter` Filter out too rows with too few or too many entries.
* Can place an iterator after a TwoTableIterator (meaning higher priority). Useful iterators:
  * `emitEmptyEntries` Choose whether to emit entries with a Value of an empty byte array ""
  * `emitZeroEntries` Choose whether to emit entries with a Value encoding "0"

