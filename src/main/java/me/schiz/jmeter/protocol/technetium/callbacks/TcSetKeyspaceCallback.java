package me.schiz.jmeter.protocol.technetium.callbacks;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.atomic.AtomicInteger;

public class TcSetKeyspaceCallback implements AsyncMethodCallback<Cassandra.AsyncClient.set_keyspace_call> {
    private static final Logger log = LoggingManager.getLoggerForClass();
    protected AtomicInteger flag;

    public static int PENDING_STATUS = 0;
    public static int SUCCESS_STATUS = 1;
    public static int FAILURE_STATUS = 2;

    protected Object monitor;

    public TcSetKeyspaceCallback(AtomicInteger flag, Object monitor) {
        this.flag  = flag;
        this.monitor = monitor;
    }

    @Override
    public void onComplete(Cassandra.AsyncClient.set_keyspace_call response) {
        log.warn("set keyspace ok");
        synchronized (monitor) {
            //response.write_args();
            flag.set(SUCCESS_STATUS);
            monitor.notify();
        }
    }

    @Override
    public void onError(Exception exception) {
        log.warn("set keyspace exception " , exception);
        System.out.println(exception);
        synchronized (monitor) {
            flag.set(FAILURE_STATUS);
            monitor.notify();
        }
    }
}
