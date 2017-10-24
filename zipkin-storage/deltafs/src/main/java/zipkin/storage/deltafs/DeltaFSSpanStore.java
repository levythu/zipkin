package zipkin.storage.deltafs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.lang.Throwable;
import java.util.TreeMap;
import javax.annotation.Nullable;
import zipkin.DependencyLink;
import zipkin.Span;
import zipkin.Codec;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageAdapters;
import zipkin.storage.QueryRequest;

import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;
import static zipkin.internal.GroupByTraceId.TRACE_DESCENDING;
import static zipkin.internal.Util.sortedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DeltaFSSpanStore implements SpanStore {
  // private final SortedMultimap<Pair<Long>, Span> spansByTraceIdTimeStamp =
  //     new LinkedListSortedMultimap<>(VALUE_2_DESCENDING);
  //
  // /** This supports span lookup by {@link zipkin.Span#traceId lower 64-bits of the trace ID} */
  // private final SortedMultimap<Long, Pair<Long>> traceIdToTraceIdTimeStamps =
  //     new LinkedHashSetSortedMultimap<>(Long::compareTo);
  // /** This is an index of {@link Span#traceId} by {@link zipkin.Endpoint#serviceName service name} */
  // private final ServiceNameToTraceIds serviceToTraceIds = new ServiceNameToTraceIds();
  // /** This is an index of {@link Span#name} by {@link zipkin.Endpoint#serviceName service name} */
  // private final SortedMultimap<String, String> serviceToSpanNames =
  //     new LinkedHashSetSortedMultimap<>(String::compareTo);

  private static final Logger LOG = LoggerFactory.getLogger(DeltaFSSpanStore.class);

  private final boolean strictTraceId;
  final int maxSpanCount;
  volatile int acceptedSpanCount;

  private DeltaFSShuffler deltafsShuffler;

  // Historical constructor
  public DeltaFSSpanStore() {
    this(new DeltaFSStorage.Builder());
    deltafsShuffler = DeltaFSShuffler.GetInstance();
    if (deltafsShuffler == null) LOG.error("DeltaFSRPC not ready!");
  }

  DeltaFSSpanStore(DeltaFSStorage.Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.maxSpanCount = builder.maxSpanCount;
    deltafsShuffler = DeltaFSShuffler.GetInstance();
    if (deltafsShuffler == null) LOG.error("DeltaFSRPC not ready!");
  }

  final StorageAdapters.SpanConsumer spanConsumer = new StorageAdapters.SpanConsumer() {
    @Override public void accept(List<Span> spans) {
      if (spans.isEmpty()) return;
      if (spans.size() > maxSpanCount) {
        spans = spans.subList(0, maxSpanCount);
      }
      addSpans(spans);
    }

    @Override public String toString() {
      return "DeltaFSSpanConsumer";
    }
  };

  private void warnNotImpl() {
    LOG.warn("The query for deltafs is not implemented. And it will return dummy result.");
  }

  @Deprecated
  public synchronized List<Long> traceIds() {
    List<Long> placeholder = new LinkedList<Long>();
    warnNotImpl();
    return placeholder;
  }

  synchronized void addSpans(List<Span> spans) {
    // TODO: to implement
    deltafsShuffler.batchAppendStart();
    for (Span sp : spans) {
      try {
        String idStr = Long.toString(sp.id);
        String json = new String(Codec.JSON.writeSpan(sp), "UTF-8");
        deltafsShuffler.append("traces", idStr, json);
        String spanNamePair = idStr + "/" + sp.name;
        deltafsShuffler.appendHost("services", "me", spanNamePair, "localhost");
      } catch (Throwable e) {
        LOG.warn("Exception in appending spans: " + e.toString());
      }
    }
    deltafsShuffler.batchAppendEnd();
    return;
  }

  @Override public List<List<Span>> getTraces(QueryRequest request) {
    List<List<Span>> placeholder = new LinkedList<List<Span>>();
    warnNotImpl();
    return placeholder;
  }

  @Override public synchronized List<Span> getTrace(long traceId) {
    List<Span> placeholder = new LinkedList<Span>();
    warnNotImpl();
    return placeholder;
  }

  @Override public synchronized List<Span> getTrace(long traceIdHigh, long traceIdLow) {
    List<Span> placeholder = new LinkedList<Span>();
    warnNotImpl();
    return placeholder;
  }

  @Override public synchronized List<Span> getRawTrace(long traceId) {
    List<Span> placeholder = new LinkedList<Span>();
    warnNotImpl();
    return placeholder;
  }

  @Override public synchronized List<Span> getRawTrace(long traceIdHigh, long traceId) {
    List<Span> placeholder = new LinkedList<Span>();
    warnNotImpl();
    return placeholder;
  }

  @Override
  public synchronized List<String> getServiceNames() {
    List<String> placeholder = new LinkedList<String>();
    warnNotImpl();
    return placeholder;
  }

  @Override
  public synchronized List<String> getSpanNames(String service) {
    List<String> placeholder = new LinkedList<String>();
    warnNotImpl();
    return placeholder;
  }

  @Override
  public synchronized List<DependencyLink> getDependencies(long endTs, @Nullable Long lookback) {
    List<DependencyLink> placeholder = new LinkedList<DependencyLink>();
    warnNotImpl();
    return placeholder;
  }
}
