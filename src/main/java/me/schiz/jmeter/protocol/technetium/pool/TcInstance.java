package me.schiz.jmeter.protocol.technetium.pool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;

public class TcInstance {
    private static volatile long _id = 0;
    private volatile long id;

    protected TNonblockingSocket transport;
    protected Cassandra.AsyncClient asyncClient;
    protected volatile boolean state;

    public TcInstance(String host, int port, int timeout, TAsyncClientManager clientManager, TProtocolFactory protocolFactory) throws IOException {
        transport = new TNonblockingSocket(host, port, timeout);
        asyncClient = new Cassandra.AsyncClient(protocolFactory, clientManager, transport);
        state = false;
        id = ++_id;
    }

    public TTransport getTransport() {
        return this.transport;
    }

    public Cassandra.AsyncClient getClient() {
        return asyncClient;
    }

    public long getId() {
        return id;
    }

    public void release() {
        state = true;
    }
}