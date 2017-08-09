package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.skvi.MapIterator;
import edu.mit.ll.graphulo.util.IteratorAdapter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * A version of {@link org.apache.accumulo.core.client.ClientSideIteratorScanner}
 * for {@link BatchScanner}s.  Scans all entries into a SortedMap at the client, then iterates over the SortedMap
 * into the iterators set after construction of the ClientSideIteratorAggregatingScanner.
 */
public class ClientSideIteratorAggregatingScanner extends ScannerOptions implements BatchScanner {
  BatchScanner bs;

  public ClientSideIteratorAggregatingScanner(BatchScanner bs) {
    this.bs = bs;
  }


  @Override
  public void setRanges(Collection<Range> ranges) {
    bs.setRanges(ranges);
  }

  @Override
  public Iterator<Map.Entry<Key, Value>> iterator() {
    SortedMap<Key,Value> allEntriesMap = new TreeMap<>();
    for (Map.Entry<Key, Value> entry : bs) {
      allEntriesMap.put(entry.getKey(), entry.getValue());
    }
    SortedKeyValueIterator<Key, Value> skvi = new MapIterator(allEntriesMap);

    final SortedMap<Integer,IterInfo> tm = new TreeMap<>();
    for (IterInfo iterInfo : serverSideIteratorList) {
      tm.put(iterInfo.getPriority(), iterInfo);
    }

    try {
      skvi = IteratorUtil.loadIterators(skvi, tm.values(), serverSideIteratorOptions, new IteratorEnvironment() {
        @Override
        public SortedKeyValueIterator<Key, Value> reserveMapFileReader(final String mapFileName) throws IOException {
          return null;
        }

        @Override
        public AccumuloConfiguration getConfig() {
          return null;
        }

        @Override
        public IteratorUtil.IteratorScope getIteratorScope() {
          return null;
        }

        @Override
        public boolean isFullMajorCompaction() {
          return false;
        }

        @Override
        public void registerSideChannel(final SortedKeyValueIterator<Key, Value> iter) {
        }

        @Override
        public Authorizations getAuthorizations() {
          return bs.getAuthorizations();
        }

        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
          return null;
        }

        @Override
        public boolean isSamplingEnabled() {
          return false;
        }

        @Override
        public SamplerConfiguration getSamplerConfiguration() {
          return null;
        }
      }, false, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new IteratorAdapter(skvi);
  }

  @Override
  public Authorizations getAuthorizations() {
    return bs.getAuthorizations();
  }

  @Override
  public synchronized void fetchColumnFamily(Text col) {
    bs.fetchColumnFamily(col);
  }

  @Override
  public synchronized void fetchColumn(Text colFam, Text colQual) {
    bs.fetchColumn(colFam, colQual);
  }

  @Override
  public void fetchColumn(IteratorSetting.Column column) {
    bs.fetchColumn(column);
  }

  @Override
  public synchronized void clearColumns() {
    bs.clearColumns();
  }

  @Deprecated
  @Override
  public synchronized SortedSet<Column> getFetchedColumns() {
    throw new UnsupportedOperationException("fetched columns not tracked");
  }

  @Override
  public void setTimeout(long timeout, TimeUnit timeUnit) {
    bs.setTimeout(timeout, timeUnit);
  }

  @Override
  public long getTimeout(TimeUnit timeunit) {
    return bs.getTimeout(timeunit);
  }

  @Override
  public void close() {
    bs.close();
  }
}
