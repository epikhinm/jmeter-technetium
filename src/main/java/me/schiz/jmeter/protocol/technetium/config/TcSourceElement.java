package me.schiz.jmeter.protocol.technetium.config;

import me.schiz.jmeter.protocol.technetium.pool.TcPool;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TcSourceElement extends ConfigTestElement
        implements TestStateListener, TestBean {
    private static final Logger log = LoggingManager.getLoggerForClass();

    protected static ConcurrentHashMap<String, TcPool> pools = null;
    protected String source;
    protected String keyspace;
    protected String hosts;
    protected int   maxInstances;
    protected int   maxSelectors;
    protected int   timeout;

    private static int DEFAULT_PORT = 9160;


    public int getTimeout() {
        return maxSelectors;
    }
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    public int getMaxSelectors() {
        return maxSelectors;
    }
    public void setMaxSelectors(int maxSelectors) {
        this.maxSelectors = maxSelectors;
    }
    public int getMaxInstances() {
        return maxInstances;
    }
    public void setMaxInstances(int maxInstances) {
        this.maxInstances = maxInstances;
    }
    public String getSource() {
        return source;
    }
    public void setSource(String source) {
        this.source = source;
    }
    public String getKeyspace() {
        return keyspace;
    }
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }
    public String getHosts() {
        return hosts;
    }

    public List<AbstractMap.SimpleEntry<String, Integer>> getHostsAsEntries() {
        String[] rawEntries = hosts.split(";");
        ArrayList<AbstractMap.SimpleEntry<String, Integer>> list = new ArrayList<AbstractMap.SimpleEntry<String, Integer>>();

        for(String row : rawEntries) {
            String[] cols = row.split(":");
            int port;
            try{
                port = cols.length > 1 ? Integer.parseInt(cols[1]) : DEFAULT_PORT;
            } catch (NumberFormatException e) {
                port = DEFAULT_PORT;
            }
            list.add(new AbstractMap.SimpleEntry<String, Integer>(cols[0], port));
        }
        return list;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public static TcPool getSource(String source) {
        TcPool pool = pools.get(source);
        if(pool == null)    log.warn("not found pool `" +source + "`");
        return pool;
    }

    public TcSourceElement() {
        if(pools == null) {
            synchronized (this.getClass()) {
                if(pools == null) {
                    pools = new ConcurrentHashMap<String, TcPool>();
                }
            }
        }
    }

    @Override
    public void testStarted() {
        if(pools.contains(source))  log.warn("TcSource `" +  source + "` already created");
        else {
            try {
                pools.put(source, new TcPool(getKeyspace() ,maxSelectors, maxInstances, timeout));

                //add hosts for pool
                for(AbstractMap.SimpleEntry<String, Integer> row : getHostsAsEntries()) {
                    pools.get(source).addServer(row.getKey(), row.getValue());
                }

                log.info("added new source `" + source + "` (s:" +maxSelectors + ",i: " + maxInstances + ",t: "+ timeout + ")");
            } catch (IOException e) {
                log.error("cannot create TcPool" + e.getMessage());
            }
        }
    }

    @Override
    public void testStarted(String s) {
        testStarted();
    }

    @Override
    public void testEnded() {
        for(String row : pools.keySet()) {
            pools.get(row).shutdown();
            log.info("shutdown pool `" + row + "`");
        }
    }

    @Override
    public void testEnded(String s) {
        testEnded();
    }
}
