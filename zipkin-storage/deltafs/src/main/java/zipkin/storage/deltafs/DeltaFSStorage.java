package zipkin.storage.deltafs;

import static zipkin.storage.StorageAdapters.blockingToAsync;
import zipkin.storage.StorageComponent;
import zipkin.Component;
import zipkin.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zipkin.storage.AsyncSpanStore;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.StorageAdapters;

/**
 * Test storage component that keeps all spans in memory, accepting them on the calling thread.
 */
public final class DeltaFSStorage implements StorageComponent {

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder implements StorageComponent.Builder {
    boolean strictTraceId = true;
    int maxSpanCount = 500000;

    /** {@inheritDoc} */
    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    public Builder maxSpanCount(int maxSpanCount) {
      this.maxSpanCount = maxSpanCount;
      return this;
    }

    @Override
    public DeltaFSStorage build() {
      return new DeltaFSStorage(this);
    }
  }

  final DeltaFSSpanStore spanStore;
  final AsyncSpanStore asyncSpanStore;
  final AsyncSpanConsumer asyncConsumer;

  // Historical constructor
  public DeltaFSStorage() {
    this(new Builder());
  }

  DeltaFSStorage(Builder builder) {
    spanStore = new DeltaFSSpanStore(builder);
    asyncSpanStore = blockingToAsync(spanStore, Runnable::run);
    asyncConsumer = blockingToAsync(spanStore.spanConsumer, Runnable::run);
  }

  @Override public DeltaFSSpanStore spanStore() {
    return spanStore;
  }

  @Override public AsyncSpanStore asyncSpanStore() {
    return asyncSpanStore;
  }

  public StorageAdapters.SpanConsumer spanConsumer() {
    return spanStore.spanConsumer;
  }

  @Override
  public AsyncSpanConsumer asyncSpanConsumer() {
    return asyncConsumer;
  }

  @Override public CheckResult check() {
    return CheckResult.OK;
  }

  @Override public void close() {
  }
}
