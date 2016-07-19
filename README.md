Graphulo
========

Graphulo is a Java library for the [Apache Accumulo][] database delivering server-side sparse matrix math primitives that enable higher-level graph algorithms and analytics.

Graph primitives loosely follow the [GraphBLAS][] spec.
Example algorithms include Breadth First Search, finding a k-Truss subgraph, 
computing Jaccard coefficients, transforming by [TF-IDF][],
and performing non-negative matrix factorization. 
We encourage developers to use Graphulo's primitives to build 
more algorithms and applications.

Graphulo's design resembles that of a stored procedure in classic relational databases.
The client calls (directly or through delagate functions)
the OneTable or TwoTable core functions, 
which create new tables to store results in Accumulo
rather than gather results at the client.
Both functions scan Accumulo with iterators that themselves
open Scanners and BatchWriters, allowing reading from multiple tables
and writing to multiple tables in one client call, 
as opposed to ordinary scans that read fconfrom a single table 
and send results back to the client.

Graphulo is tested on Accumulo 1.6 and 1.7.

[Apache Accumulo]: https://accumulo.apache.org/
[GraphBLAS]: http://istc-bigdata.org/GraphBlas/
[TF-IDF]: https://en.wikipedia.org/wiki/Tf%E2%80%93idf

### How do I get started?
Look at the material in the `docs/` folder, especially the Use and Design slide deck.
Read and run the examples-- see below for how.

### Directory Structure

<pre>
src/                                 
  assembly/...        Files for building graphulo.
  main/               Main code and resources. Included in output JAR.
    java/...          
    resources/        Contents copied into output JAR.
      log4j.xml       Logging configuration for clients at runtime.
  test/               Test code and resources. Not included in output JAR.
    java/...             
    resources/        Contents available for tests and examples.
      log4j.xml       Logging configuration for tests and examples.
      data/...        Data folder - contains pre-created graphs.

target/
  graphulo-${version}.jar         Graphulo binaries, enough for client usage.
  graphulo-${version}-alldeps.jar Graphulo + all referenced binaries, for easy server installation.
  graphulo-${version}-libext.zip  ZIP of the JARs of all dependencies.
  site/apidocs/...                Javadoc.
  graphulo-${version}-dist.zip    Distribution ZIP. Contains all source and binaries.

docs/...              Papers and presentations related to Graphulo.
  
pom.xml               Maven Project Object Model. Defines how to build graphulo.
post-test.bash        Script to display output of tests from shippable/testresults.
lessMiniServerLogs.sh Script to display server-side MiniAccumulo logs for most recent singleton test.
tailMiniServerLogs.sh Similar to above.
deploy.sh             Script to deploy a graphulo build to Accumulo and Matlab D4M.
README.md             This file.
README-D4M.md         Readme for d4m_api_java, also included in this distribution.
example.conf          Example configuration file for testing.

   -Below files only in git repository-
.gitignore            Files and folders to exclude from git.
.travis.yml           Enables continuous integration testing.
shippable.yml         Enables continuous integration testing.
</pre>

[Project Object Model]: https://maven.apache.org/guides/introduction/introduction-to-the-pom.html

### Build
[![Shippable Build Status](https://api.shippable.com/projects/54f27f245ab6cc13528fd44d/badge?branchName=master)](https://app.shippable.com/projects/54f27f245ab6cc13528fd44d/builds/latest)
[![Travis Build Status](https://travis-ci.org/Accla/graphulo.svg?branch=master)](https://travis-ci.org/Accla/graphulo)

Prerequisite: Install [Maven](https://maven.apache.org/download.cgi).

Run `mvn package -DskipTests` to compile and build graphulo.
This creates three primary Graphulo artifacts inside the `target/` sub-directory:

1. `graphulo-${version}.jar`         Graphulo binaries, enough for client usage.
Include this on the classpath of Java client applications that call Graphulo functions. 
2. `graphulo-${version}-alldeps.jar` Graphulo + all referenced binaries, for easy server installation.
Include this in the `lib/ext/` directory of Accumulo server installations 
in order to provide the Graphulo code and every class referenced by Graphulo,
so that the Accumulo instance has everything it could possibly need to instantiate Graphulo code.
3. `graphulo-${version}-libext.zip`  ZIP of the JARs of all dependencies.
Unzip this in a D4M installation directory to make Graphulo available in D4M.  

The maven script will build everything on Unix-like systems (including Mac),
as long as the *zip* utility is installed.
On Windows systems, `DBinit.m` may not be built (used in D4M installation). 
See the message in the build after running `mvn package`.

#### Other Build Options

Run `mvn package -DskipTests -DNoDoAll` to not build the alldeps jar, libext zip, and javadoc.
This build will be much faster than normal builds.

Run `mvn package -DskipTests -DBundleAll` to create a jar that includes the Accumulo dependencies 
alongside all other dependencies. This should not be used in an Accumulo installation because
the Accumulo classes will conflict with the jars in the Accumulo installation.
It is useful for running standalong programs.

### Test
Tests only run on Unix-like systems.

* `mvn test` to run tests on [MiniAccumulo][], 
a portable, lightweight Accumulo instance started before and stopped after each test class.
* `mvn test -DTEST_CONFIG=local` to run tests on a local instance of Accumulo.
See TEST_CONFIG.java for changing connection parameters, such as testing on a remote Accumulo instance.
  * When running on a full Accumulo instance, if a test fails for any reason, it
  may leave a test table on the Accumulo instance which will mess up future tests.  
  Delete the tables manually if this happens.
* `post-test.bash` is a utility script to output test results to the console.
* Test results are saved in the `shippable/testresults` folder.
* Run `mvn clean` to delete output from previously run tests.


### Develop
If you're interested in using an IDE for development, [IntelliJ][] is a good choice.
However, IntelliJ's profiler listens by default on port 10001, which conflicts with Accumulo's
[default master replication service port](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_network).
You can fix this by manually setting the port in the `bin/idea.sh` file in your IntelliJ installation
according to the [instructions posted here](https://stackoverflow.com/questions/13345986/intellij-idea-using-10001-port).
Otherwise if you don't fix it, you may not be able to run IntelliJ and Accumulo concurrently.
You could also change Accumulo's ports from their default.


#### Testing on Standalone Accumulo via a configuration file
Another way to specify an Accumulo instance for running tests on, 
if not through the shortcut keywords defined in TEST_CONFIG.java,
is by passing `-DTEST_CONFIG=filepath` to `mvn test`.
The file specified in the path should look like this template
(it is best to choose a user with all system permissions like root):

    accumulo.it.cluster.standalone.admin.principal=username
    accumulo.it.cluster.standalone.admin.password=password
    accumulo.it.cluster.standalone.zookeepers=localhost:2181
    accumulo.it.cluster.standalone.instance.name=instance

The default filepath is `./GraphuloTest.conf`. If this file exists and has the correct properties inside, 
then it is loaded and used instead of MiniAccumulo if no TEST_CONFIG is specified.

These parameters may also be defined on the command line as in

    mvn test -Daccumulo.it.cluster.standalone.admin.principal=bla -Daccumulo.it.cluster.standalone.admin.password=bla ...

[MiniAccumulo]: https://accumulo.apache.org/1.7/accumulo_user_manual.html#_mini_accumulo_cluster

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
2. `AdjBFSExample` -- inserts a SCALE 10 adjacency table with degree table,
creates a new table with the union sum of three steps of Breadth First Search, 
and counts the number of resulting entries.
3. `EdgeBFSExample` -- similar to #2, using an incidence table with degree table.
4. `SingleBFSExample` -- similar to #2, using a single-table schema.
5. `JaccardExample` -- inserts a SCALE 10 adjacency table with degree table, 
applies a low-pass filter to subset the table and forces it undirected,
creates a new table holding Jaccard coefficients,
and calculates statistics over the Jaccard coefficients.
6. `NMFExample` -- Fringe example factoring a table into two tables W and H.

### Deploy to Accumulo and D4M
Execute `./deploy.sh`. This script will do the following:

1. ACCUMULO DEPLOY: Copy the `alldeps` Graphulo JAR into `$ACCUMULO_HOME/lib/ext`, the external library folder of your Accumulo installation,
so that Accumulo can use Graphulo's server-side iterators.
2. D4M DEPLOY: Copy the client Graphulo JAR into `$D4M_HOME/lib`, unpack dependencies into `$D4M_HOME/libext`
and update `$D4M_HOME/matlab_src/DBinit.m`.

Feel free to delete or edit parts of the script for deploying to your environment.

### Distribute
Execute `mvn install -DskipTests` to create a *fourth artifact*:
the distribution zip file `target/graphulo-${version}-dist.zip`.
This zip contains all source code and javadoc.
Binaries may be built with `mvn package -DskipTests`, 
and the new distribution may even be used to create further distributions using `mvn install`.

`mvn install` will also install the three main Graphulo artifacts (plus the POM) 
into your local maven repository (typically the directory `~/.m2/repository/edu/mit/ll/graphulo/`),
which enables using Graphulo as a Maven dependency in a derivative client project. 
As a summary, these are:

1. `graphulo-${version}.jar` 
2. `graphulo-${version}-alldeps.jar` 
3. `graphulo-${version}-libext.zip`
4. `graphulo-${version}.pom`

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
graphulo.AdjBFS("Atable", "v0,", 3, "Rtable", null, null, -1, 
                "ADegtable", "deg", false, 5, 15);
```

See Examples above for more elaborate client code usage.

#### Creating custom addition and multiplication operators

##### Simple custom operators
If your operator meets the below requirements, you're in luck!  
You can implement your operation by creating a Java subclass of [SimpleTwoScalar][].
Define the method `Value multiply(Value Aval, Value Bval)`,
and you are set to use your operator in the context of a matrix multiply, 
matrix element-wise multiply or sum, Combiner sum, 
or even unary function application if you fix one of the operands.

1. Operator logic only acts on Values; no knowledge of the operands' Keys necessary.
2. Operator returns exactly zero or one Value result. 
The multiply method should return `null` if zero results are desired (e.g., filter operations).

Even in this simpler model, some advanced logic is possible.
Override the `init` method to take options provided from the client at runtime 
through [IteratorSetting][] options that configure your operator, or setup initial state.

Very simple example of a SimpleTwoScalar: 
[ConstantTwoScalar][] ignores its operand Values and always returns a constant.
The constant is "1" by default and can be changed through IteratorSetting options.

Very useful SimpleTwoScalar: [MathTwoScalar][].
It can do standard arithmetic +, *, -, /, min, max, and power on its two operands.


##### Advanced custom operators
To create a custom addition operator, 
create a Java class implementing [SortedKeyValueIterator][] with the addition logic.
You will probably want to subclass [Combiner][].
Then pass an [IteratorSetting][] to the Graphulo functions,
which will apply the iterator to result tables.
Addition is *lazy*, in the sense that the addition runs when a table is scanned or compacted,
 which may occur significantly after an operation finishes.
Do not remove addition iterators from a table until every entry in the table
is summed together, which one can guarantee by running a full major compaction.

There are three kinds of custom multiplication operations, each with its own interface you can implement:
 
1. Matrix multiplication. Multiplies entries with matching row, column family and qualifier.
See the interface [MultiplyOp][].
2. Element-wise multiplication, also used for element-wise sum. A more accurate title is element-wise collision function.
See the interface [EWiseOp][].
3. Unary function application.  See the interface [ApplyOp][].  
Alternatively, create a [SortedKeyValueIterator][] directly.

Some use cases for implementing advanced operator logic are if your logic

1. manipulates returned Keys in non-standard ways 
(i.e., changes the row and column qualifier differently than matrix multiply or element-wise multiply would); 
2. returns more than one entry as the result of a single operator call;
3. performs some setup or other function based on more stateful knowledge, 
such as what the contents of one row of the input tables \[`ONEROWA` or `ONEROWB` (default)\] 
or both rows of the input tables \[`TWOROW`\] held in memory during matrix multiplication
(implement [RowStartMultiplyOp][] in this case); or 
4. (very advanced) performs a non-standard pattern of multiplying two matching rows, 
different from the Cartesian product of the two rows' entries
(implement [RowMultiplyOp][] in this case). 

[SortedKeyValueIterator]: https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/iterators/SortedKeyValueIterator.html
[IteratorSetting]: https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/client/IteratorSetting.html
[MultiplyOp]: src/main/java/edu/mit/ll/graphulo/mult/MultiplyOp.java
[EWiseOp]: src/main/java/edu/mit/ll/graphulo/ewise/EWiseOp.java
[ApplyOp]: src/main/java/edu/mit/ll/graphulo/apply/ApplyOp.java
[RowStartMultiplyOp]: src/main/java/edu/mit/ll/graphulo/mult/RowStartMultiplyOp.java
[SimpleTwoScalar]: src/main/java/edu/mit/ll/graphulo/simplemult/SimpleTwoScalar.java
[ConstantTwoScalar]: src/main/java/edu/mit/ll/graphulo/simplemult/ConstantTwoScalar.java
[RowMultiplyOp]: src/main/java/edu/mit/ll/graphulo/mult/RowMultiplyOp.java
[Combiner]: https://accumulo.apache.org/1.7/apidocs/index.html?org/apache/accumulo/core/iterators/Combiner.html

### How to use Graphulo in Matlab client code with D4M
The following code snippet is a good starting point for using Graphulo,
assuming the D4M libraries are also installed:

```Matlab
G = DBaddJavaOps('edu.mit.ll.graphulo.MatlabGraphulo','instance','localhost:2181','root','secret');
res = G.AdjBFS('Atable','v0;v7;v9;',3,'Rtable','','ADegtable','OutDeg',false,5,15);
```

### How to use Graphulo with Column Visibilities and Authorizations
Authorizations are used to scan entries in an Accumulo table.
Entries have an empty column visibility by default, which means that 
a user scanning the table with empty authorizations can see those entries.
Every user has permission to use empty authorizations.

In order to scan entries that have a non-empty column visibility, 
the scanning user must have permission to place an authorization on the scan
that satisfies each entries' visibility, which takes the form of a Boolean algebra of labels.
A user's permitted authorizations can be set via
`connector.securityOperations().changeUserAuthorizations(user, newAuthorizations);`.
These authorizations can then be passed to the Connector methods that create Scanners and BatchScanners.

Each Graphulo function takes an Authorizations object as an argument,
 which is passed to every (Batch)Scanner created by Graphulo, 
 including the ones created server-side 
 (in this case, the Authorizations are transmitted via iterator options).

Some Graphulo capabilities create new entries and ingest them into Accumulo tables.
This includes matrix multiply, element-wise sum and multiply, and some ApplyOp 
and other SKVIs. 
*These functions will inherit the visibility of their parent Key when possible.*
In particular, it is possible to inherit parent key visibility for EWiseOp, ApplyOp and
EdgeBFSMultiply.  MultiplyOp in general does not have a clear inheritance for column visibility,
because generated keys descend from two parent keys that may have differing visibility.

An additional feature these functions take is an argument called `newVisibility`
that sets the visibility of all newly created Keys to the given constant visibility.
This overrides the visibility of any parent keys and forces generated keys to have the given visibility.

If more fine-grained control of visibility creation is desired, please implement a custom 
MultiplyOp, EWiseOp, ApplyOp or more general SKVI as applicable.

### Debugging
Before debugging a problem, consider 

1. checking the Accumulo monitor, running by default on <http://localhost:50095/>;
2. printf or log4j debugging;
3. debugging by deletion, i.e., if removing a piece of code solves your problem, 
then you know where the problem is;
4. inserting the Accumulo system `DebugIterator` or the Graphulo `DebugInfoIterator` into the iterator stack,
which log information about every call from the iterator above it at the `DEBUG` or `INFO` level respectively.

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
TwoTableIterator configured with ROW mode and emitNoMatchEntries=false.
* SpEWiseX uses TwoTableIterator connected to a RemoteSourceIterator on table A and a local iterator on table B.
TwoTableIterator configured with EWISE mode and emitNoMatchEntries=false.
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
`null`      | (-inf,+inf)
`:,`        | (-inf,+inf)
`:,c,`      | (-inf,c]
`f,:,`      | [f,+inf)
`b,:,g`     | [b,g]
`b,:,g,x`   | [b,g] U [c,c)
`x,`        | [x,x)
`x,z,`      | [x,x) U [z,z)
`x,z,:`     | [x,x) U [z,+inf)
`,`         | ["","")     (the empty string row)
`,a,`       | ["","") U [a,a)
`,:,b,f,:,` | ["",b] U [f,+inf)

The "separator" character in D4M strings is arbitrary. The above examples use `,` but could just as easily
have used the tab character, the newline character, or the null `\0` character.
The key is to pick a separator character that never occurs elsewhere in the D4M String.
It is also forbidden to reference a row that consists of the single character `:`.

As a standard, the empty string and null always have the same semantic meaning
for functions that take string arguments. Differentiating between them would create great confusion.

### Iterators

##### Important Points
Be careful about Iterator names and priorities.  
Each iterator must have a unique name and unique priority for each diScopes
(scan, minor compaction, and major compaction).
Iterator priority determines the order in which iterators are run.
Mkae sure iterators run in the correct order intended.
 
By default, combiners placed on result tables run at priority 6, 
whereas the main OneTable and TwoTable operations run at priority 7.
These can be user-configured.

Using the same name for two iterators on the same table in the same diScopes
may result in exceptions at best and logical errors at worst.
Be especially careful with the name of a DynamicIterator.

Sometimes iterators need to apply to a limited number of diScopes.
OneTable and TwoTable have a special rule for respecting this for combiners applied to the results table.
When an iterator is given to One- or TwoTable with the special option "_ONSCOPE_",
Graphulo parses option value as a D4M string with the keywords "scan", "minc", and/or "majc".
If at least one of them is present, then Graphulo will only add the iterator to the present diScopes.
Otherwise Graphulo adds the iterator to all three diScopes.
Use `GraphuloUtil.addOnScopeOption` to do this automatically.

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
* `authorizations` Authorizations to use in the Scanner in serialized form, e.g. from `authorizations.serialize()`
* `diter.*` Encapsulated form of a DynamicIteratorSetting. See the code for how to use. 
These encoded iterators are placed on the remote table scan, running on the remote Accumulo server at an encoded priority.
These also encode iterator options for each iterator.

##### TwoTableIterator
* `A. ... ` All the options of RemoteSourceIterator, to read table A from a remote Accumulo table. 
Don't specify when operating on a single table.  Same for B.
* `dot` Either "ROW" or "EWISE" or "NONE".
* `multiplyOp` Name of class that implements EWiseOp, for "EWISE". 
* `multiplyOp.opt.OPTION_NAME` An option supplied to the multiply class's `init` function.
* `rowMultiplyOp` Name of class that implements RowMultiplyOp, for "ROW".
* `rowMultiplyOp.opt.OPTION_NAME` An option supplied to the multiply class's `init` function.
* `AT.emitNoMatch` controls whether entries in A that do not match entries in B are emitted anyway.
Use false for ordinary table multiply (intersection of entries); true for sum (union of entries).
* `rowMultiplyOp.opt.alsoDoAA` controls whether the cartesian row multiplier also multiplies `A*A`.
Set `AT.emitNoMatch` to true when `alsoDoAA` is true, so that rows of A that do not match rows of B
are considered.  Conversely, if `AT.emitNoMatch` is true but `alsoDoAA` is false, then
individual entries from A are emitted (`A*A` is not computed).

##### LruCacheIterator
Pre-sums entries that collide within a fixed size cache. An optimization. See code for how to use.

##### RemoteWriteIterator
* `reducer` Used to "collect" something to send to the client. Name of class that implements `Reducer` interface. 
The final state of the reducer is sent to the client once the scan finishes, or when at a checkpoint.
The reduce operation must therefore be commutative and associative.
* `reducer.opt.OPTION_NAME` An option supplied to the Reducer's `init` function.
* `checkpointNumEntries` Emit a monitoring entry after this number of entries. Not working presently for EWiseX. 
* `checkpointTime` (in milliseconds) More useful than NumEntries.
* `tableName`
* `tableNameTranspose`

##### DynamicIterator
Sometimes one wants to use multiple iterators in a chain in a place where there is a spot only for a single iterator.
This is DynamicIteratorSetting's use case. It loads multiple iterators in a list with options at the Accumulo server.
For example, the following code creates an iterator at priority 6
that first applies a Combiner to sum values together 
and then filters away zero and negative values:
  
```Java
IteratorSetting sumFilterOp =
  new DynamicIteratorSetting()
  .append(MathTwoScalar.combinerSetting(1, null, ScalarOp.PLUS, ScalarType.DOUBLE))
  .append(MinMaxValueFilter.iteratorSetting(1, ScalarType.DOUBLE, Double.MIN_NORMAL, Double.MAX_VALUE))
  .toIteratorSetting(6);
```

## Other 

We encourage the use case of *queued* analytics that run on a table subset,
as opposed to *whole table* analytics that are better off using a filesystem than a database.
We therefore prioritize low latency over high throughput when tradeoffs are necessary.

