package me.schiz.jmeter.protocol.technetium.pool;

import org.apache.thrift.async.TAsyncClientManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class TcClientManagerPool {

    protected ArrayList<TAsyncClientManager> managersList;

    public TcClientManagerPool(int maxSize) throws IOException {
        managersList = new ArrayList<TAsyncClientManager>(maxSize);
        for(int i=0; i< maxSize;++i) {
            managersList.add(new TAsyncClientManager());
        }
    }

    public TAsyncClientManager getClientManager() {
        return managersList.get(ThreadLocalRandom.current().nextInt(managersList.size()));
    }

    public void shutdown() {
        for(TAsyncClientManager clientManager : managersList) {
            clientManager.stop();
        }
    }
}