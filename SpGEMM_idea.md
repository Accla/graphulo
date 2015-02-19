SpGEMM Iterator Concepting
--------------------------
All triples are in the form (row,colQualifier,value).

## API for C += A *.+ B
`TableMult(A, Arows, BT, BTrows, C, colFilter, plusOp, multOp)`

e.g. `TableMult(A, 'doc1,doc3,doc5,:,doc7,', ':', BT, 'doc1,:,doc7,doc9,', 'C', '', 'plus', 'times')`

1. A: 		Accumulo table reference
2. Arows: 	rows of A to take as subset (in Java, a sorted list of Ranges)
3. BT: 		Accumulo table reference - Transpose of B
4. BTrows:	rows of BT (=columns of B) to take as subset (in Java, a sorted list of Ranges)
5. C:       Accumulo table reference, or string: the name of a new table to create.
If C is an existing table, *sums* the result of A*B to the existing entries in C.
6. colFilter: a subset of columns of A and columns of BT to use in the matrix multiply.
Leave blank for all rows.
Not as efficient a selection as Arows or BTrows, espeically if colFilter contains a range.
A list of single columns can be explicitly fetched but a range requires scanning over all columns
and discarding the ones not in the range. 
7. plusOp:	string that connects to a kind of summing operation, like 'plus', 'min', 'max', 'xor', 'union', 'plusmod7', ... (in Java, an enumeration)
8. multOp:	string that connects to a kind of multiplication operation, like 'times', 'and', 'intersect', 'plus', ... (in Java, an enumeration)

Note that the Arows, BTrows and colFilter options almost generalize **SpRef**.

## Java Iterator Implementation
Recall that an iterators form a chain of processing, starting with the inital
priority 0 / system iterators that read from the actual table data and emit tuples
of the form **(row,colF,colQ,visibility,ts,val)** to a child iterator, going up priorities
until we reach the last iterator (kind of priority infinity) which sends tuples to the client.

Create the new table C if it does not already exist.
Implement the following iterator stack.

    RemoteRowIterator from Table A
	| emits whole rows of A
	v
	DotRemoteIterator from Table BT
	| emits entries after multiplication
	|
	v      NormalTableEntries after VersioningIterator
	o<____/ (merge)
	|
	v
	SummingIterator
	|
	v
	FinalResult
	


The first iterator `RemoteRowIterator`

1. Scan whole rows from remote table A of the form
(Arow,"","","",Ats,Ablob)
where Ablob is a sorted list of all the tuples in an entire row from remote table A,
i.e., Ablob expands to a list of (Arow,AcolF,AcolQ,Avis,Ats,Aval) entries.
We get this whole row by attaching a scan-time `WholeRowIterator` to the scanner for table A.
2. Emit the whole row tuples unchanged.
3. Continue scanning whole rows from A until there are no more, then finish.

The second iterator `DotRemoteIterator`

1. Receive whole row A tuples from its parent iterator.  Decode it.
2. Set `fetchColumns(...)` on a BatchScanner for table BT to only receive the columns that exist in the whole row A.
3. Scan entries from remote table BT of the form
(BTrow,BTcolF,BTcolQ,BTvis,BTts,BTval).
4. Multiply BTval with Aval as they match AcolF==BTcolF and AcolQ==BTcolQ.
5. Emit the entry
(Arow,colF,BTrow,"",ts,multval)
where colF is the matching column family, "" means empty visibility,
ts is a newly generated timestamp via `System.currentTimeMillis()`,
and multval is the value after multiplication.
6. Continue scanning entries from BT until there are no more,
then receive a new whole row from A from the parent iterator until there are no more,
then finish. (this is a two-level loop)

We run these iterators once by manually initiating a compaction on table C
with the iterators set for one-time execution.

If Accumulo kills an iterator, Accumulo restarts it with a seek command to its last returned value.
We need to implement the iterators to recover their state from that information.
For RemoteRowIterator, start scanning the row after the last one emitted.
For DotRemoteIterator, get a whole row for table A from the parent and
start scanning at the next whole row from table BT.

### Options
We can store options for both iterators in their configuration on table C.


4. Which *plusOp* and *multOp* to use.

## List of Java Iterators

### RemoteSourceIterator
Returns entries from a table. Replaces/ignores entries from the parent, if present.

Options:

1. Options for which remote table to connect to.
 * `zookeeperHost` address and port
 * `timeout` Zookeeper timeout between 1000 and 300000 (enforced in Accumulo source code)
 * `instanceName` instance name
 * `tableName` Remote Table name
 * `username`
 * `password` !!! Need a better option; stored in plaintext.
 * Username and password (or some other way of authenticating)
2. Which rows of the table to scan, i.e. a list of Ranges.
 * `doWholeRow` boolean
 * `rowRanges` Matlab format
3. (Maybe) Which columns of the table to include. Cannot have ranges
(or at least, we would have to scan all the columns and then ignore the ones outside the range,
so less efficient than taking a subset of rows).
 * `fetchColumns`

### RemoteMergeIterator
Merges a `RemoteSourceIterator` into a standard SKVI stack.

Options are all the options that a `RemoteSourceIterator` takes, 
with their keys prepended with the string "RemoteSource."

### (Todo) DotRemoteSourceIterator
Holds a `RemoteSourceIterator` for A and a `RemoteSourceIterator` for BT.
Reads whole rows of A and entries of BT.

Options prepended by "A." and "BT." go to their remote tables.
Also takes a "mult" option.

## Discussion
Why use two iterators instead of a single iterator?

* More, smaller iterators mean each one is simpler and more task-focused,
possibly reusable if we make them generic.
On the other hand, less, bigger iterators may be more efficient in their computation
(less passing around of intermediate data values, more stream-like instead of buffering an
entire row).
* Emitting an entire row from remote table A acts like a kind of 'save state' in a nice position.
If the first iterator is killed,
it restarts at the start of the current row (or the start of the next row if finished),
as opposed to needing to restart a scan in the middle of a row.
* Big iterators that take a lot of time and/or thread resources
seem more likely to get killed before making progress.

We can implement by creating one big iterator,
and we can also implement by creating many small iterators.
Tradeoff: code mangagement/understandability vs. optimization/performance.

Performance: Place table splits on C equal to the table splits on A.
With enough tablet servers, this should make iterators for all the tablets of C
scan all the tablets of A in parallel.

Visibility is up for grabs.  Right now it is ignored.

We need to use a `Scanner` to maintain sorted order of scan results.
`BatchScanner` may not return results in sorted order.
Maybe `Scanner` will use less threads.

Remote DB connection properties: 
`org.apache.accumulo.core.conf.Property` lists a bunch of properties part of the Accumulo configuration.  
We may be able to use some of these instead of passing all the connection information over the iterator configuration, if we are connecting to a table in the same Accumulo instance.
I don't think this is public API.
Also see `ClientConfiguration.loadDefault()`. 

Password-- here are some alternatives to putting the password on plaintext on the Accumulo table configuration (which of note, can only be accessed by people who can read the Accumulo table configuration)

* Public key or something similar? I can't find the correct classes in Accumulo, but 1.6 supposedly brought in new ways to authenticate.
* If on the same instance, get login credentials from the place Accumulo stores them (in salted form?) (non-public API probably) 
* If on the same instance, read Accumulo properties? (non-public API probably)
* Setup some direct connection to a tablet server inside Accumulo is executing the iterator anyway. (lots of work, bypasses many useful mechanisms Accumulo has)
 

Table C will only have a single column family "",
treating all included column familes as part of the same dot product.
An alternative is to seperate/distinguish the matrix multiply for each column family,
so that we do not combine different column families.

Alternative: *Insert the iterator configuration into the actual table* instead of storing it in the iterator options.  Let the iterators read it from their parent iterator (the first iterator's parent is the table data itself). Pass along the configuration data for future iterators in the values returned by those iterators. This is more complicated; recommend we use the iterator options and gague its performance.

Alternative: *Iterate through the row in A and BT instead of processing entire rows at once.*
We would have to do this if we cannot fit a row in memory.

Alternative: *Table Cloning.* Clone table A instead of opening a scanner to it.
Ignore (don't emit) the rows of the cloned table that are outside a given subset of rows of A.

Alternative: *YARN.*  Do processing on HDFS RFiles directly, outside of Accumulo.
Better option if the table multiplication is large. Not sure for small jobs.

Alternative: *Invert the model.* Plave one-time compaction iterators on A and BT
that *write* their entries to the appropriate places in C.
An iterator on C finishes the job by doing the *sum* and *multiply* operations.

We assume BT exists.

Hard to allow for arbitrary plus and mult operations, because that requires shipping arbitrary code.
Better strategy to provide a number of useful plus and mult operations and restrict the client to those.

This method does not require a daemon process on the Accumulo nodes. It's iterator-only.

### Implementation notes
* Be careful with seeking a `WholeRowIterator` in the middle of a row-- it will skip to the next row.
I don't think it is a problem because Accumulo should not re-seek an iterator in the middle of a row.
* `IteratorUtils.transformedIterator(...)` may come in handy for chaning returned `Key`s.
* `Scanner.getReadaheadThreshold()` is configurable.  also `Scanner.setBatchSize()`.
* Scanner spawns a class set with its options on calling `iterator()`. Ok.
* `IsolatedScanner` vs. `Scanner.setIsolation(true)`?

## First Steps
1. Created `InjectIterator` that injects hardcoded values into an Accumulo table.
It uses the side channel, but should work fine using a regular merge-in.
2. Create `RemoteIterator` which ignores its parent source and instead reads out another Accumulo table.
Tested with AND without the `WholeRowIterator`. Pass on both MiniAccumulo and standalone single-node Accumulo.
Important to increase the Zookeeper timeout above 1000 ms.  I set it to 5000.

## Todo
* Test `RemoteMergeIterator`
* Test timeout now that it is not set to -1. Default is 1000ms.
* Refactor such that no "RemoteMergeIterator.PREFIX_RemoteIterator" required.
* Implement fetchColumns with ranges.
* Implement rowRanges.