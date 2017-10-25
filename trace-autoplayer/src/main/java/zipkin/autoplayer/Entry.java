package zipkin.autoplayer;

import java.lang.Integer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.Throwable;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import javax.annotation.Nullable;

import zipkin.Span;
import zipkin.Codec;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageComponent;
import zipkin.storage.InMemoryStorage;
import static zipkin.storage.Callback.NOOP;
import zipkin.storage.Callback;
import zipkin.storage.deltafs.DeltaFSStorage;

public class Entry {

  private StorageComponent getTestedStorageComponent() {
    // In-mem:
    return InMemoryStorage.builder()
      .strictTraceId(true)
      .maxSpanCount(Integer.MAX_VALUE)
      .build();

    // DeltaFS:
    // return DeltaFSStorage.builder()
    //   .strictTraceId(true)
    //   .maxSpanCount(Integer.MAX_VALUE)
    //   .build();
  }

  public static int parseCmdLine(String[] args, int pos, int def) {
    if (args.length <= pos) return def;
    try {
        return Integer.parseInt(args[pos]);
    } catch (NumberFormatException e) {
        return def;
    }
  }

  private BufferedReader source = null;
  public int readLines(int count, String[] res) throws Throwable {
    int current = 0;
    while (current < count) {
      res[current] = source.readLine();
      if (res[current] == null) {
        return current;
      }
      current++;
    }

    return count;
  }

  public Long task = 0L;
  public Long lastTS = 0L;
  public long totalRecord = 0L;

  public void putInStorage(String[] str, int len, StorageComponent storage) {
    ArrayList<Span> spans = new ArrayList<Span>();
    for (int i = 0; i < len; i++) {
      spans.add(Codec.JSON.readSpan(str[i].getBytes()));
    }
    synchronized (task) {
      task = task + 1;
    }
    storage.asyncSpanConsumer().accept(spans, new Callback<Void>() {
      @Override public void onSuccess(@Nullable Void value) {
        long endTS = (new Date()).getTime();
        synchronized (task) {
          task = task - 1;
          lastTS = endTS;
        }
      }

      @Override public void onError(Throwable t) {
      }
    });
    // TODO: sleep?
  }

  public void Run(String file, int times, int blockSize) throws Throwable {
    System.out.println(
        "Replaying traces from " + file + Integer.toString(times) +
        "times, with blocksize = " + Integer.toString(blockSize));

    StorageComponent storage = getTestedStorageComponent();

    String[] res = new String[blockSize];
    while (true) {
      if (source == null) {
        if (times == 0) {
          return;
        }
        times--;
        source = new BufferedReader(new FileReader(file));
      }
      int actualLen = readLines(blockSize, res);
      totalRecord += actualLen;
      if (actualLen > 0)
        putInStorage(res, actualLen, storage);

      if (actualLen < blockSize) {
        source = null;
      }
    }
  }

  public static void main(String[] args) throws Throwable {
      System.out.println("Hello, World");
      String file = args[0];
      int times = parseCmdLine(args, 1, 1);
      int blockSize = parseCmdLine(args, 2, 100);

      Entry me = new Entry();
      long startTS = (new Date()).getTime();
      me.Run(file, times, blockSize);
      System.out.println("Sleep");
      while (true) {
        TimeUnit.SECONDS.sleep(1);
        synchronized (me.task) {
          if (me.task == 0) {
            System.out.println("Done, total time = " + Long.toString(me.lastTS - startTS));
            System.out.println("Total records = " + Long.toString(me.totalRecord));
            System.out.println("Throughput = " + Double.toString((  (double)me.totalRecord) * 1000 / (double)(me.lastTS - startTS)  ));
          }
        }
      }
  }

}
