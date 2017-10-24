package zipkin.autoplayer;

import java.lang.Integer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.Throwable;

import zipkin.Span;
import zipkin.Codec;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageComponent;
import zipkin.storage.InMemoryStorage;

public class Entry {

  private StorageComponent getTestedStorageComponent() {
    return InMemoryStorage.builder()
      .strictTraceId(true)
      .maxSpanCount(Integer.MAX_VALUE)
      .build();
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

  public void Run(String file, int times, int blockSize) throws Throwable {
    System.out.println(
        "Replaying traces from " + file + Integer.toString(times) +
        "times, with blocksize = " + Integer.toString(blockSize));
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
      me.Run(file, times, blockSize);
  }

}
