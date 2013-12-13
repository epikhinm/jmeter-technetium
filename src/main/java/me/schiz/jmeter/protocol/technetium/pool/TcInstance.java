package me.schiz.jmeter.protocol.technetium.pool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;

public class TcInstance {
    public TNonblockingSocket transport;
    public Cassandra.AsyncClient asyncClient;

    public TcInstance(String host, int port, int timeout, TAsyncClientManager clientManager, TProtocolFactory protocolFactory) throws IOException {
        transport = new TNonblockingSocket(host, port, timeout);
        asyncClient = new Cassandra.AsyncClient(protocolFactory, clientManager, transport);
    }

    public TTransport getTransport() {
        return this.transport;
    }

    public Cassandra.AsyncClient getClient() {
        return asyncClient;
    }
}