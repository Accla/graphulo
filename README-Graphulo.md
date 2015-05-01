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
No guarantees are made for compatibility with other versions.

[Accumulo]: https://accumulo.apache.org/
[GraphBLAS]: http://istc-bigdata.org/GraphBlas/


### Building
[![Build Status](https://api.shippable.com/projects/54f27f245ab6cc13528fd44d/badge?branchName=master)](https://app.shippable.com/projects/54f27f245ab6cc13528fd44d/builds/latest)
[![Build Status](https://travis-ci.org/Accla/d4m_api_java.svg)](https://travis-ci.org/Accla/d4m_api_java)

Graphulo uses maven for building and will work for all systems for installing into Accumulo.

For installing into Matlab D4M, one step of the build process is Linux-dependent.
On non-Linux systems, DBinit.m may not be built. See the message in the build after running `mvn package`.

Run `mvn package -DskipTests=true` to compile and build a JAR for distribution. 
The JAR is placed in the `target` subdirectory.

### Testing
* `mvn test` to run tests on [MiniAccumulo][],
a miniature version of Accumulo that enables testing without a full Accumulo installation.
Probably only works on Linux.
* `mvn test -DTEST_CONFIG=local` to run tests on a local instance of Accumulo.
See or edit TEST_CONFIG.java to define other options.
* `post-test.bash` is a utility script to output test results to the console.

[MiniAccumulo]: https://accumulo.apache.org/1.6/accumulo_user_manual.html#_mini_accumulo_cluster

## Using as a Java Library

### Installing into Accumulo
Copy the JAR into the `lib/ext` subdirectory of your Accumulo installation directory.

### Using in Java client code
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

## Using from Matlab

### Installing into Matlab D4M
1. Copy `d4m_api_java/target/d4m_api_java-VERSION.jar` into `d4m_api/lib`.
2. Extract target/libext-VERSION.zip into `d4m_api`.
3. Move `d4m_api/DBinit.m` into `d4m_api/matlab_src`.

### Using in Matlab client code
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


### Graphulo Files
The `d4m_api_java/src/main/resources` subdirectory contains files that are copied into the 
`d4m_api_java-VERSION.jar` build.
Of note, the `log4j.xml` file defines logging performed by Matlab.
Accumulo installations do not use this logging as Accumulo logging is defined by the 
config files in the `logs/` subdirectory of Accumulo's installation.

All Java classes are in the package `edu.mit.ll.graphulo` or sub-packages.

The following is a list of the main classes in Graphulo. This does not include test classes.
Classes marked "REMOVE." will be removed before release (this is a todo list for Dylan).

<table class="packageSummary" border="0" cellpadding="3" cellspacing="0" summary="Interface Summary table, listing interfaces, and an explanation">
<caption><span>Interface Summary</span><span class="tabEnd">&nbsp;</span></caption>
<tr>
<th class="colFirst" scope="col">Interface</th>
<th class="colLast" scope="col">Description</th>
</tr>
<tbody>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/IGraphulo.html" title="interface in edu.mit.ll.graphulo">IGraphulo</a></td>
<td class="colLast">
<div class="block">REMOVE. Graphulo interface</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/IMultiplyOp.html" title="interface in edu.mit.ll.graphulo">IMultiplyOp</a></td>
<td class="colLast">
<div class="block">Multiplication operation on 2 entries.</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/SaveStateIterator.html" title="interface in edu.mit.ll.graphulo">SaveStateIterator</a></td>
<td class="colLast">
<div class="block">An iterator that can reconstruct its state by signalling a special Key,Value to emit.</div>
</td>
</tr>
</tbody>
</table>
<table class="packageSummary" border="0" cellpadding="3" cellspacing="0" summary="Class Summary table, listing classes, and an explanation">
<caption><span>Class Summary</span><span class="tabEnd">&nbsp;</span></caption>
<tr>
<th class="colFirst" scope="col">Class</th>
<th class="colLast" scope="col">Description</th>
</tr>
<tbody>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/BadHardListIterator.html" title="class in edu.mit.ll.graphulo">BadHardListIterator</a></td>
<td class="colLast">REMOVE. Deprecated</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/BranchIterator.html" title="class in edu.mit.ll.graphulo">BranchIterator</a></td>
<td class="colLast">
<div class="block">An abstract parent class for custom computation merged into a regular SKVI stack.</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/DebugInfoIterator.html" title="class in edu.mit.ll.graphulo">DebugInfoIterator</a></td>
<td class="colLast">
<div class="block">For debugging; sends information about iterator calls to log4j at INFO level.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/DotIterator.html" title="class in edu.mit.ll.graphulo">DotIterator</a></td>
<td class="colLast">
<div class="block">Multiply step of outer product, emitting partial products.</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/Graphulo.html" title="class in edu.mit.ll.graphulo">Graphulo</a></td>
<td class="colLast">
<div class="block">Holds a <code>Connector</code> to an Accumulo instance for calling core client Graphulo operations.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/GraphuloUtil.html" title="class in edu.mit.ll.graphulo">GraphuloUtil</a></td>
<td class="colLast">
<div class="block">Utility functions</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/HardListIterator.html" title="class in edu.mit.ll.graphulo">HardListIterator</a></td>
<td class="colLast">
<div class="block">For testing; an iterator that emits entries from a list of hardcoded data.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/InjectIterator.html" title="class in edu.mit.ll.graphulo">InjectIterator</a></td>
<td class="colLast">
<div class="block">For testing; interleaves data from a <a href="../../../../edu/mit/ll/graphulo/BadHardListIterator.html" title="class in edu.mit.ll.graphulo"><code>BadHardListIterator</code></a> with parent iterator entries.</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/MatlabGraphulo.html" title="class in edu.mit.ll.graphulo">MatlabGraphulo</a></td>
<td class="colLast">
<div class="block">REMOVE. Matlab interface to Graphulo.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/PeekingIterator2.html" title="class in edu.mit.ll.graphulo">PeekingIterator2</a>&lt;E&gt;</td>
<td class="colLast">
<div class="block">Caches two entries.</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/PeekingIterator3.html" title="class in edu.mit.ll.graphulo">PeekingIterator3</a>&lt;E&gt;</td>
<td class="colLast">
<div class="block">Caches three entries.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/RemoteMergeIterator.html" title="class in edu.mit.ll.graphulo">RemoteMergeIterator</a></td>
<td class="colLast">
<div class="block">Merge a RemoteSourceIterator into a regular SKVI iterator stack.</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/RemoteSourceIterator.html" title="class in edu.mit.ll.graphulo">RemoteSourceIterator</a></td>
<td class="colLast">
<div class="block">Reads from a remote Accumulo table.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/RemoteWriteIterator.html" title="class in edu.mit.ll.graphulo">RemoteWriteIterator</a></td>
<td class="colLast">
<div class="block">SKVI that writes to an Accumulo table.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/SmallLargeRowFilter.html" title="class in edu.mit.ll.graphulo">SmallLargeRowFilter</a></td>
<td class="colLast">
<div class="block">Used for on-the-fly degree filtering with min and max degree.</div>
</td>
</tr>
<tr class="altColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/TableMultIterator.html" title="class in edu.mit.ll.graphulo">TableMultIterator</a></td>
<td class="colLast">
<div class="block">SpGEMM on Accumulo tables: C += A * B.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../edu/mit/ll/graphulo/Watch.html" title="class in edu.mit.ll.graphulo">Watch</a>&lt;K extends java.lang.Enum&lt;K&gt;&gt;</td>
<td class="colLast">
<div class="block">For measuring performance: spans and counters.</div>
</td>
</tr>
</tbody>
</table>

Package `edu.mit.ll.graphulo.mult`:

<table class="packageSummary" border="0" cellpadding="3" cellspacing="0" summary="Class Summary table, listing classes, and an explanation">
<caption><span>Class Summary</span><span class="tabEnd">&nbsp;</span></caption>
<tr>
<th class="colFirst" scope="col">Class</th>
<th class="colLast" scope="col">Description</th>
</tr>
<tbody>
<tr class="altColor">
<td class="colFirst"><a href="../../../../../edu/mit/ll/graphulo/mult/BigDecimalMultiply.html" title="class in edu.mit.ll.graphulo.mult">BigDecimalMultiply</a></td>
<td class="colLast">
<div class="block">Decode values as BigDecimal objects, multiply and re-encode the result.</div>
</td>
</tr>
<tr class="rowColor">
<td class="colFirst"><a href="../../../../../edu/mit/ll/graphulo/mult/LongMultiply.html" title="class in edu.mit.ll.graphulo.mult">LongMultiply</a></td>
<td class="colLast">
<div class="block">Decode values as Long objects, multiply and re-encode the result.</div>
</td>
</tr>
</tbody>
</table>

