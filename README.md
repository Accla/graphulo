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

Graphulo is tested on Accumulo 1.6 and 1.7 

[Accumulo]: https://accumulo.apache.org/
[GraphBLAS]: http://istc-bigdata.org/GraphBlas/


### Directory Structure

An asterisk indicates files/folders recommended to change, or not made yet.

<pre>
src/                                 
  assembly/...        Files for building graphulo.
  main/               Main code and resources. Included in output JAR.
    java/...          
    java-templates/...  Code from d4m_api_java that needs preprocessing for Accumulo 1.6-1.7 compatibility.
    resources/        Contents copied into output JAR.
      log4j.xml       Logging configuration for clients at runtime.
  test/               Test code and resources. Not included in output JAR.
    java/...             
    resources/        Contents available for tests and examples.
      log4j.xml       Logging configuration for tests and examples.
      data/...        Data folder - contains pre-created graphs.

target/
  graphulo-${version}.jar         Binaries.
  graphulo-${version}-libext.zip  ZIP of the JARs of all dependencies.
  site/apidocs/...                Javadoc.
  graphulo-${version}-dist.zip    Distribution ZIP. Contains all source and binaries.

docs/...              Papers and presentations related to Graphulo.
  
pom.xml               Maven Project Object Model. Defines how to build graphulo.
post-test.bash        Script to display output of tests from shippable/testresults.
deploy.sh             Script to deploy a graphulo build to Accumulo and Matlab D4M.
README.md             This file.
README-D4M.md         Readme for d4m_api_java, also included in this distribution.

   -Below files only in git repository-
.gitignore            Files and folders to exclude from git.
.travis.yml           Enables continuous integration testing.
shippable.yml         Enables continuous integration testing.
</pre>

[Project Object Model]: https://maven.apache.org/guides/introduction/introduction-to-the-pom.html

### Build
[![Shippable Build Status](https://api.shippable.com/projects/54f27f245ab6cc13528fd44d/badge?branchName=master)](https://app.shippable.com/projects/54f27f245ab6cc13528fd44d/builds/latest)
[![Travis Build Status](https://travis-ci.org/Accla/d4m_api_java.svg)](https://travis-ci.org/Accla/d4m_api_java)

Prerequisite: Install [Maven](https://maven.apache.org/download.cgi).

Run `mvn package -DskipTests=true` to compile and build graphulo.
Compilation runs against 1.7 by default.
The main distribution files are a JAR containing graphulo code, 
and a libext ZIP file containing dependencies for both projects.
The JAR and ZIP are created inside the `target/` directory.

Run `mvn package -Paccumulo1.6 -DskipTests=true` to explicitly compile against Accumulo 1.6,
but this should not be necessary because Accumulo 1.7 is backward-compatible with 1.6.
More generally, append `-Paccumulo1.6` to any mvn command to act on Accumulo 1.7. 

The maven script will build everything on Unix-like systems.
On Windows systems, `DBinit.m` may not be built (used in D4M installation). 
See the message in the build after running `mvn package`.

### Test
Tests only run on Unix-like systems.

* `mvn test` to run tests on [MiniAccumulo][], 
a portable, lightweight Accumulo instance started before and stopped after each test class.
* `mvn test -DTEST_CONFIG=local` to run tests on a local instance of Accumulo.
See TEST_CONFIG.java for changing connection parameters, such as testing on a remote Accumulo instance.
* `post-test.bash` is a utility script to output test results to the console.
* Test results are saved in the `shippable/testresults` folder.
* Run `mvn clean` to delete output from previously run tests.

[MiniAccumulo]: https://accumulo.apache.org/1.6/accumulo_user_manual.html#_mini_accumulo_cluster

### Examples
The classes in [`src/test/java/edu/mit/ll/graphulo/examples/`](src/test/java/edu/mit/ll/graphulo/examples/)
contain simple, well-commented examples of how to use Graphulo.
To run an example, use the command `mvn test -Dtest=TESTNAME`, replacing `TESTNAME` with the name of the test.
To run every example, use the command `mvn test -Dtest=*Example`.
View example output with `./post-test.bash`, or inspect the `shippable/testreusults/` directory.

Examples run inside [MiniAccumulo][] by default. To run an example in a local Accumulo instance,
add the option `-DTEST_CONFIG=local` to the Maven command.
Running examples in a local instance has the advantage of retaining input and result tables 
in the cluster, so that you may inspect them more closely after the example finishes.

Here is a list of included examples:

1. `TableMultExample` -- inserts two SCALE 10 adjacency tables,
stores the result of multiplying them, and counts the number of resulting entries.
2. `AdjBFSExample` -- inserts a SCALE 10 adjacency table,
creates a new table with the union sum of three steps of Breadth First Search, 
and counts the number of resulting entries.
3. `EdgeBFSExample` -- similar to #2, using an incidence table.
4. `SingleBFSExample` -- similar to #2, using a single-table schema.

### Deploy to Accumulo and D4M
Execute `./deploy.sh`. This script will do the following:

1. ACCUMULO DEPLOY: Copy the Graphulo JAR into `$ACCUMULO_HOME/lib/ext`, the external library folder of your Accumulo installation,
so that Accumulo can use Graphulo's server-side iterators.
2. D4M DEPLOY: Copy the Graphulo JAR into `$D4M_HOME/lib`, unpack dependencies into `$D4M_HOME/libext`
and update `$D4M_HOME/matlab_src/DBinit.m`.

Feel free to delete or edit parts of the script for deploying to your environment.

### Distribute
Execute `mvn install` to build the distribution zip file `target/graphulo-${version}-dist.zip`.
This zip contains all source code, javadoc and compiled binaries.

`mvn install` will also install `target/graphulo-${version}.jar` into your local maven repository,
which enables using Graphulo as a Maven dependency in a derivative client project. 


### How to use Graphulo in Java client code
Include Graphulo's JAR in the Java classpath when running client code.  
This is automatically done if 

1. Your client project is a maven project;
2. `mvn install` is run;
3. and you add the following to your client project's pom.xml:
```
<dependency>
  <groupId>edu.mit.ll</groupId>
  <artifactId>graphulo</artifactId>
  <version>${version}</version>
</dependency>
```

The following code snippet is a good starting point for using Graphulo:

```java
// setup
Instance instance = new ZooKeeperInstance(INSTANCE_NAME, INSTANCE_ZK_HOST);
Connector connector = instance.getConnector(USERNAME, PASSWORD_TOKEN);
Graphulo graphulo = new Graphulo(connector, PASSWORD_TOKEN);

// call Graphulo functions...
graphulo.AdjBFS("Atable", "v0,", 3, "Rtable", null, "ADegtable", "deg", false, 5, 15);
```

See Examples above for more elaborate client code usage.

#### Creating custom addition and multiplication operators
To create a custom addition operator, 
create a Java class implementing [SortedKeyValueIterator][] with the addition logic.
Then pass an [IteratorSetting][] to the Graphulo functions,
which will apply the iterator to result tables.
Addition is *lazy*, in the sense that the addition runs when a table is scanned or compacted,
 which may be significantly after an operation finishes.
Do not remove addition iterators from a table until every entry in the table
is summed together, which one can guarantee by performing a full major compaction.

To create a custom multiplication operation, 
create a Java class implementing [IMultiplyOp][] with the multiplication logic.
For simple TableMult multiplication, you can extend the [SimpleMultiply][] class instead of implementing the full interface.
For simple element-wise multiplication, you can extend the [SimpleEWiseX][] class instead of implementing the full interface.
See the classes in the `edu.mit.ll.graphulo.mult` package for examples.
Non-simple multiplication that should implement [IMultiplyOp][] directly are multiplication logic that

1. manipulates returned Keys in non-standard ways; 
2. takes init options (passed from the client through `multiplyOp.opt.OPTION_NAME`); 
3. needs to perform some setup or other function based on viewing an entire row in memory; or 
4. returns more than one entry per multiplication.

[SortedKeyValueIterator]: https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/iterators/SortedKeyValueIterator.html
[IteratorSetting]: https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/client/IteratorSetting.html
[IMultiplyOp]: src/main/java/edu/mit/ll/graphulo/mult/IMultiplyOp.java
[SimpleMultiply]: src/main/java/edu/mit/ll/graphulo/mult/SimpleMultiply.java
[SimpleEWiseX]: src/main/java/edu/mit/ll/graphulo/mult/SimpleEWiseX.java

### How to use Graphulo in Matlab client code with D4M
The following code snippet is a good starting point for using Graphulo,
assuming the D4M libraries are also installed:

```Matlab
G = DBaddJavaOps('edu.mit.ll.graphulo.MatlabGraphulo','instance','localhost:2181','root','secret');
res = G.AdjBFS('Atable','v0;v7;v9;',3,'Rtable','','ADegtable','OutDeg',false,5,15);
```

### Debugging
Before debugging a problem, consider 

1. checking the Accumulo monitor, running by default on <http://localhost:50095/>;
2. printf or log4j debugging;
3. debugging by deletion, i.e., if removing a piece of code solves your problem, 
then you know where the problem is.

Debuggers can be extraordinarily helpful once running but challenging to set up.
There are two methods to use a debugger: 

1. starting a "debug server" in your IDE
and having a Java application connect to it at startup, or 
2. having a Java application start a "debug server" 
and listen for you to make a connection from your IDE.
 
#### Debugging Standalone Accumulo
I tend to use method (1) for standalone Accumulo, not for any particular reason.
Run the following before launching Accumulo via its start scripts or via `accumulo tserver &`:

```bash
export ACCUMULO_TSERVER_OPTS="-agentlib:jdwp=transport=dt_socket,server=n,address=127.0.0.1:5005,suspend=y" 
```

This will instruct the JVM for the Accumulo tablet server to connect to your IDE,
which you should configure to listen on port 5005 (of 127.0.0.1).

You must debug quickly in this mode.  If you idle too long without making a debug step,
then the Accumulo tablet server will lose its Zookeeper lock and kill itself.

#### Debugging MiniAccumulo
Set `-DTEST_CONFIG=miniDebug` for any test using MiniAccumulo.
The code will print to `System.out` the ports that MiniAccumulo randomly chooses to listen on 
for each Accumulo process after starting MiniAccumulo.
You have 10 seconds to connect to one of the ports (probably the TABLET_SERVER port)
before the test continues executing.
I recommend using `tail -f shippable/testresults/edu.mit.ll.graphulo.TESTNAME-output.txt`
to see the ports as they are printed.  Replace `TESTNAME` with the name of the test you are runnning.

For a bit more insight, see the `before()` method of [MiniAccumuloTester][].

[MiniAccumuloTester]: src/test/java/edu/mit/ll/graphulo/MiniAccumuloTester.java

## Implementation

Note: the current implementation only has SpGEMM and BFS completely tested. 
The rest are partially implemented ideas.

### GraphBLAS mapping
* SpGEMM uses TwoTableIterator connected to a RemoteSourceIterator on table AT and a local iterator on table B.
TwoTableIterator configured with ROW_CARTESIAN_PRODUCT and emitNoMatchEntries=false.
* SpEWiseX uses TwoTableIterator connected to a RemoteSourceIterator on table A and a local iterator on table B.
TwoTableIterator configured with ROW_COLF_COLQ_MATCH and emitNoMatchEntries=false.
* SpEWiseSum uses TwoTableIterator connected to a RemoteSourceIterator on table A and a local iterator on table B.
TwoTableIterator configured with no multiplication and emitNoMatchEntries=true.
PreSumCacheIterator is important for efficiency.
* Sparse -- insert from client to table.
* Find -- scan from table to client.
* SpRef -- use RemoteWriteIterator with rowRanges and colFilter to output results to another table.
* SpAsgn -- unimplemented. An awkward primitive.
* Apply -- use an iterator with the function to apply + RemoteWriteIterator.
* Reduce -- use a RemoteWriteIterator's `reduce` function.

### D4M String Representation of Ranges
Throughout Graphulo, we make use of a string format that represents a collection of Accumulo Ranges.
Here are several examples of this format and the ranges they represent:

D4M String  | Range
------------|-----------------
`:,`        | (-inf,+inf)
`:,c,`      | (-inf,c]
`f,:,`      | [f,+inf)
`b,:,g`     | [b,g]
`b,:,g,x`   | [b,g] U [c,c)
`x,`        | [x,x)
`x,z,`      | [x,x) U [z,z)
`x,z,:`     | [x,x) U [z,+inf)

The "separator" character in D4M strings is arbitrary. The above examples use `,` but could just as easily
have used the tab character, the newline character, or the null `\0` character.
The key is to pick a separator character that never occurs elsewhere in the D4M String.
It is also forbidden to reference a row that consists of the single character `:`.


### Iterators

##### RemoteSourceIterator
* `rowRanges` Row ranges to fetch from remote Accumulo table, Matlab syntax. (default ":," all) 
* `colFilter` String representation of column qualifiers, e.g. "a,b,c,".
Four modes of operation:
  1. Blank `colFilter`: do nothing.
  2. No ranges `colFilter`: use scanner.fetchColumn() which invokes an Accumulo system ColumnQualifierFilter.
  3. Singleton range `colFilter`: use Accumulo user ColumnSliceFilter.
  4. Multi-range `colFilter`: use Graphulo D4mColumnRangeFilter.
* `zookeeperHost` Address and port, e.g. "localhost:2181". Future: extract from Accumulo config if not provided
* `timeout` Zookeeper timeout between 1000 and 300000 (default 1000). Future: extract from Accumulo config if not provided
* `instanceName`
* `tableName`
* `username`
* `password` Anyone who can read the Accumulo table config or log files will see the password in plaintext.
* `iter.7` Class name of an iterator to place on the remote table scan, running on the remote Accumulo server at the specified priority. 
Run as many as desired, each with its own priority.
* `iter.7.OPTION_NAME` An option supplied to a remote iterator.

##### TwoTableIterator
* `B. ... ` All the options of RemoteSourceIterator, to read table A from a remote Accumulo table. 
Don't specify when operating on a single table.
* `(A/B).emitNoMatchEntries` Both false for multiply (intersection of entries); both true for sum (union of entries)
* `dot` Either "ROW_CARTESIAN_PRODUCT" or "ROW_COLF_COLQ_MATCH" or nothing.
* `multiplyOp` Name of class that implements IMultiplyOp. 
* `multiplyOp.opt.OPTION_NAME` An option supplied to the multiply class's `init` function.

##### Future: PreSumCacheIterator
* `combiner` Name of class for "pre-summing" entries.
* `size` in bytes or entries?

##### RemoteWriteIterator
* `reducer` Used to "collect" something to send to the client. Name of class that implements `Reducer` interface. 
The final state of the reducer is sent to the client once the scan finishes, or when at a checkpoint.
The reduce operation must therefore be commutative and associative.
* `reducer.opt.OPTION_NAME` An option supplied to the Reducer's `init` function.
* `checkpointNumEntries` Emit a monitoring entry after this number of entries. Not working presently for EWiseX. 
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

