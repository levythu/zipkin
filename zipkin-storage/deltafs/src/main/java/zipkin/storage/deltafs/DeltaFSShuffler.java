package zipkin.storage.deltafs;

import deltafs.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test storage component that keeps all spans in memory, accepting them on the calling thread.
 */
public final class DeltaFSShuffler {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaFSShuffler.class);

  private static DeltaFSShuffler globalInstance = null;
  public synchronized static DeltaFSShuffler GetInstance() {
    if (globalInstance == null) {
      globalInstance = new DeltaFSShuffler();
    }
    return globalInstance;
  }

  //============================================================================

  public static final String[] RPC_WRAPPER_HOST = {"localhost"};
  static final int[] RPC_WRAPPER_PORT = {9090};

  private ArrayList<DeltaFSKVStore.Client> rpcSockets;

  private void connect() throws TException {
    for (int i = 0; i < RPC_WRAPPER_PORT.length; i++) {
      TTransport transport = new TSocket(RPC_WRAPPER_HOST[i], RPC_WRAPPER_PORT[i]);
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      DeltaFSKVStore.Client client = new DeltaFSKVStore.Client(protocol);
      LOG.info("DeltaFSRPC: Connect to " + RPC_WRAPPER_HOST[i] + ":" + RPC_WRAPPER_PORT[i]);
      rpcSockets.add(client);
    }
  }

  public DeltaFSShuffler() {
    rpcSockets = new ArrayList<DeltaFSKVStore.Client>();
    isBatching = false;
    try {
      connect();
      rpcSockets.get(0).append("traces", "key", "value");
      rpcSockets.get(0).get("traces", "key");
    } catch (TException e) {
      LOG.warn(e.getMessage());
    }
  }

  /****************************************************************************/
  // Write related methods

  private static boolean isBatching;

  private static ArrayList<ArrayList<String>> batchMdName;
  private static ArrayList<ArrayList<String>> batchKey;
  private static ArrayList<ArrayList<String>> batchValue;

  public static final String deliminator = ":SPL:";

  public void batchAppendStart() {
    batchKey = new ArrayList<ArrayList<String>>();
    batchValue = new ArrayList<ArrayList<String>>();
    batchMdName = new ArrayList<ArrayList<String>>();
    for (int i = 0; i < RPC_WRAPPER_PORT.length; i++) {
      batchKey.add(new ArrayList<String>());
      batchValue.add(new ArrayList<String>());
      batchMdName.add(new ArrayList<String>());
    }
    isBatching = true;
  }

  public void batchAppendEnd() {
    for (int i = 0; i < RPC_WRAPPER_PORT.length; i++) {
      try {
        rpcSockets.get(i).appendBatch(batchMdName.get(i), batchKey.get(i), batchValue.get(i));
      } catch (TException e) {
        System.err.println("ThriftException:" + e.getMessage());
        LOG.warn("ThriftException:" + e.getMessage());
      }
    }
    isBatching = false;
  }

  public void append(String mdName, String key, String val) {
    int bucketNumber = key.hashCode() % RPC_WRAPPER_HOST.length;
    appendBucket(mdName, key, val, bucketNumber);
  }

  public void appendHost(String mdName, String key, String val, String hostname) {
    for (int i = 0; i < RPC_WRAPPER_HOST.length; i++) {
      if (hostname.equals(RPC_WRAPPER_HOST[i])) {
        appendBucket(mdName, key, val, i);
        return;
      }
    }
    LOG.warn("Cannot find rpc host: " + hostname);
  }

  private void appendBucket(String mdName, String key, String val, int bucketNumber) {
    if (isBatching) {
      batchMdName.get(bucketNumber).add(mdName);
      batchKey.get(bucketNumber).add(key);
      batchValue.get(bucketNumber).add(val + deliminator);
      return;
    }
    try {
      rpcSockets.get(bucketNumber).append(mdName, key, val + deliminator);
    } catch (TException e) {
      LOG.warn("ThriftException:" + e.getMessage());
    }
  }

  /****************************************************************************/
  // Read related methods

  public ArrayList<String> get(String mdName, String key) {
    int bucketNumber = key.hashCode() % RPC_WRAPPER_HOST.length;
    return getBucket(mdName, key, bucketNumber);
  }

  public ArrayList<String> getHost(String mdName, String key, String hostname) {
    for (int i = 0; i < RPC_WRAPPER_HOST.length; i++) {
      if (hostname.equals(RPC_WRAPPER_HOST[i])) {
        return getBucket(mdName, key, i);
      }
    }
    LOG.warn("Cannot find rpc host: " + hostname);
    return new ArrayList<String>();
  }

  private ArrayList<String> getBucket(String mdName, String key, int bucketNumber) {
    try {
      String rawStr = rpcSockets.get(bucketNumber).get(mdName, key);
      String[] splitter = rawStr.split(deliminator);
      return new ArrayList<String>(Arrays.asList(splitter));
    } catch (TException e) {
      LOG.warn("ThriftException:" + e.getMessage());
      return new ArrayList<String>();
    }
  }
}
