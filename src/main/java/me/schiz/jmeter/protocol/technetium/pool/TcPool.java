package me.schiz.jmeter.protocol.technetium.pool;

import me.schiz.jmeter.protocol.technetium.callbacks.TcSetKeyspaceCallback;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TcPool {
	private static final Logger log = LoggingManager.getLoggerForClass();
    protected TcInstance[] instanceMap;
	protected AtomicBoolean[] freeInstances;
    protected String keyspace;
    protected int maxManagers;
    protected int maxInstances;

    protected TcClientManagerPool managersPool;
    protected TProtocolFactory protocolFactory;
    protected ArrayList<AbstractMap.SimpleEntry<String, Integer>> hosts;
    protected int timeout;
	protected AtomicInteger host_count;

    public TcPool(String keyspace, int maxManagers, int maxInstances, int timeout) throws IOException {
        this.keyspace = keyspace;
        this.maxManagers = maxManagers;
        this.maxInstances = maxInstances;
        this.instanceMap = new TcInstance[maxInstances];
        this.freeInstances = new AtomicBoolean[maxInstances];
		for(int i =0;i<maxInstances;i++) {
			this.freeInstances[i] = new AtomicBoolean(true); //true -- free, false -- busy
		}
		this.hosts = new ArrayList<AbstractMap.SimpleEntry<String, Integer>>();
        this.managersPool = new TcClientManagerPool(maxManagers);
        this.protocolFactory = new TBinaryProtocol.Factory();
        this.timeout = timeout;
		this.host_count = new AtomicInteger(0);

    }

	public int getFreeInstanceId() {
		int offset =  ThreadLocalRandom.current().nextInt(0, maxInstances);
		int element = -1;
		for(int i = offset; element < 0; i = (i+1) % maxInstances) {
			if(this.freeInstances[i].compareAndSet(true, false))
				element = i;
		}
		return element;
	}

    public void addServer(String host, int port) {
        synchronized (hosts) {
            for(AbstractMap.SimpleEntry<String,Integer> row : hosts) {
                if(row.getKey().equals(host) && row.getValue() == port) return;
            }
            hosts.add(new AbstractMap.SimpleEntry<String, Integer>(host, port));
        }
    }

    protected AbstractMap.SimpleEntry<String, Integer> getRandomHost() throws NotFoundHostException {
		if(hosts.isEmpty()) throw new NotFoundHostException();
		return hosts.get(host_count.getAndIncrement() % hosts.size());
    }

	public TcInstance getInstance(int instance_id) throws IOException, NotFoundHostException, TException, FailureKeySpace {
		TcInstance instance = this.instanceMap[instance_id];
		if(instance == null) {
			AbstractMap.SimpleEntry<String, Integer> host = getRandomHost();
			instance = new TcInstance(host.getKey(), host.getValue(), this.timeout, this.managersPool.getClientManager(), this.protocolFactory);

			AtomicInteger flag = new AtomicInteger(TcSetKeyspaceCallback.PENDING_STATUS);
			Object monitor = new Object();
			instance.getClient().set_keyspace(this.keyspace, new TcSetKeyspaceCallback(flag, monitor));
			try {
				synchronized (monitor) {
					while(flag.get() == TcSetKeyspaceCallback.PENDING_STATUS) { monitor.wait();}
				}
			} catch (InterruptedException e) {
				destroyInstance(instance_id);
				instance = null;
			}
			if(flag.get() == TcSetKeyspaceCallback.FAILURE_STATUS) throw new FailureKeySpace();
			instanceMap[instance_id] = instance;
		}
		return instance;
	}

    public void releaseInstance(int instance_id) throws InterruptedException {
		while(!freeInstances[instance_id].compareAndSet(false,true)) {}
    }

    public void destroyInstance(int instance_id) {
        TcInstance instance = instanceMap[instance_id];
		if(instance == null)    return;
		instance.getTransport().close();
		instanceMap[instance_id] = null;
		try {
			releaseInstance(instance_id);
		} catch (InterruptedException e) {
			log.error("can't release instance_id",e);
		}
    }

    public void shutdown() {
        for(int i =0;i<maxInstances;i++) {
			destroyInstance(i);
		}
        this.managersPool.shutdown();
    }
}