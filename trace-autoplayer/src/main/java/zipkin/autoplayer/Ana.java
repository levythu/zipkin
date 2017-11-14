package zipkin.autoplayer;

import java.lang.Integer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.Throwable;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.lang.System;

import zipkin.Span;
import zipkin.Codec;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageComponent;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.InMemoryStorage;
import static zipkin.storage.Callback.NOOP;
import zipkin.storage.Callback;

import zipkin.storage.deltafs.DeltaFSStorage;

import javax.sql.DataSource;
import zipkin.storage.mysql.MySQLStorage;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import com.zaxxer.hikari.HikariDataSource;

import zipkin.storage.cassandra3.Cassandra3Storage;

public class Ana {

  public Ana() {
    trace = new HashMap<Long, Integer>();
    spanName = new HashMap<String, Integer>();
    System.out.println("Analyzer is running");
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

  public HashMap<Long, Integer> trace;
  public HashMap<String, Integer> spanName;
  public void RunAnalysis(String file) throws Throwable {
    source = new BufferedReader(new FileReader(file));
    String[] res = new String[333333];
    int actualLen = readLines(333333, res);
    for (int i = 0; i < actualLen; i++) {
      Span sp = Codec.JSON.readSpan(res[i].getBytes());

      {
        // TraceID
        Integer oldVal = trace.get(sp.traceId);
        if (oldVal == null) {
          trace.put(sp.traceId, 1);
        } else {
          trace.put(sp.traceId, oldVal + 1);
        }
      }
      {
        // Span Name
        Integer oldVal = spanName.get(sp.name);
        if (oldVal == null) {
          spanName.put(sp.name, 1);
        } else {
          spanName.put(sp.name, oldVal + 1);
        }
      }
    }

    System.out.println("Total Spans for " + file + ": " +
                       Integer.toString(actualLen));
  }

  public void ReportAll() {
    System.out.println("SpanName:");
    for (Map.Entry<String, Integer> entry : spanName.entrySet()) {
      System.out.println(entry.getKey() + "\t" + Integer.toString(entry.getValue()));
    }
    System.out.println("TraceID:");
    for (Map.Entry<Long, Integer> entry : trace.entrySet()) {
      System.out.println(Long.toString(entry.getKey()) + "\t" + Integer.toString(entry.getValue()));
    }
  }

}
