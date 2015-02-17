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

Create the new table C and seed it by writing the dummy tuple ("","","","",Pts,""),
where Pts is the timestamp of inserting the dummy tuple.
The client performs this single entry write to table C.

If C already exists, need to experiment 

The first iterator `RemoteRowIterator`

1. Receive the dummy tuple, or existing entries from C.
If an existing entry, pass it alongside the computation so it can be added in later. Details todo.
Involves encoding the current value of C alongside the whole row returned in the next step.
2. Scans whole rows from remote table A of the form
(Arow,"","","",Ats,Ablob)
where Ablob is a sorted list of all the tuples in an entire row from remote table A,
i.e., Ablob expands to a list of (Arow,AcolF,AcolQ,Avis,Ats,Aval) entries.
We get this whole row by attaching a scan-time `WholeRowIterator` to the scanner for table A.
3. Emits tuples of the form
(Arow,"","","",Pts,Ablob)
copying in A's whole row encoding, except for the timestamp which is from the parent iterator.
4. Continue scanning whole rows from A until there are no more, then finish.

The second iterator `DotRemoteRowIterator`

1. Receives tuples from its parent iterator
2. Scans whole rows from remote table BT of the form
(BTrow,"","","",BTts,BTblob)
where BTblob expands to a sorted list of entries of the form
(BTrow,BTcolF,BTcolQ,BTvis,BTts,BTval).
3. Dot product Ablob with BTblob. That is, for each pair of entries in Ablob and BTblob
that have the same colF and colQ, *multiply* their values and *sum* all such values.
4. Emit tuples of the form
(Arow,"",BTrow,"",Pts, dot(Ablob,BTblob) ).
5. Continue scanning whole rows from BT until there are no more,
then receive a new whole row from A from the parent iterator until there are no more,
then finish.

We run these iterators once by manually initiating a compaction on table C
(after adding the dummy value) with the iterators set for one-time execution.

If Accumulo kills an iterator, Accumulo restarts it with a seek command to its last returned value.
We need to implement the iterators to recover their state from that information.
For RemoteRowIterator, start scanning the row after the last one emitted.
For DotRemoteRowIterator, get a whole row for table A from the parent and
start scanning at the next whole row from table BT.

### Options
We can store options for both iterators in their configuration on table C.

1. Options for which remote table to connect to.
 * DB address and port
 * Table name
 * Username and password (or some other way of authenticating)
2. Which rows of the table to scan, i.e. a list of Ranges.
3. (Maybe) Which columns of the table to scan. If included, it cannot have ranges
(or at least, we would have to scan all the columns and then ignore the ones outside the range,
so less efficient than taking a subset of rows).
4. Which *plusOp* and *multOp* to use.


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
I think the two-iterator setup is a nice balance.
Going with more iterators would mean a separate iterator that performs the *multiply* option,
one that performs the *sum* option, which are simple enough
that I don't think it is worth the extra data passing in memory.

Why the dummy value? I think we need it to start the first iterator.
If there are no values in table C, the first iterator may never get called.
Testing will confirm.

Performance: Place table splits on C equal to the table splits on A.
With enough tablet servers, this should make iterators for all the tablets of C
scan all the tablets of A in parallel.
We may need to alter the dummy rows so that there is a single "starter" entry at each tablet,
and add a setting to the iterator so that it knows what these starter entries are.

Timestamps set to the time the dummy row is added to table C.

Visibility is up for grabs.  Right now it is ignored.

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

## First Steps
Create a `LogIterator` that logs values (via log4j) passed in from a parent iterator
and passes them down to the next iterator. This allows us to see
what goes in and out of a custom iterator by placing a `LogIterator` before and after it.











<!-- Recycle Bin:
1. RemoteRowIterator - reads 

    iterator0 - Reads table data merged from RFiles and/or
	   |||    emits ("","","","",ts,"") dummy tuple
	   vvv    
	iterator3 - 
        v 
       ...    continues down the chain, passing tuples through each iterator
	    v
	   |||
	   vvv
	iteratorN the last iterator sends tuples over the network

We will implement CrossTableIterator similar to the following loop
```
for each (Prow,PcolF,PcolQ,Pvis,Pts,Pval) from parent iterator
  open a scanner to the remote Accumulo table
  for each (Orow,OcolF,OcolQ,Ovis,Ots,Oval) from remote table
    emit (Orow,OcolF,OcolQ

```

For every value A received from the parent iterator,
return A concatenated with every triple from a remote Accumulo table



Options:

* ReplaceRow -
 * "" for no replacement; use the row of the parent value.
 * "row" to replace with the row of the 
* WholeRow - return a row from the other table at a time instead of a triple at a time

This Java iterator emits values as the cross product
of values received from its parent iterator
and values from a remote Accumulo table.

-->
