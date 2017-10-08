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

  static final String[] RPC_WRAPPER_HOST = {"localhost"};
  static final int[] RPC_WRAPPER_PORT = {9090};

  private ArrayList<DeltaFSKVStore.Client> rpcSockets;

  public DeltaFSShuffler() {
    rpcSockets = new ArrayList<DeltaFSKVStore.Client>();
    try {
      connect();
      rpcSockets.get(0).append("123", "key", "value");
    } catch (TException e) {
      LOG.warn(e.getMessage());
    }
  }

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
}
