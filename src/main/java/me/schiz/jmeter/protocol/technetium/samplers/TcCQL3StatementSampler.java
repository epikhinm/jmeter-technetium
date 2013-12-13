package me.schiz.jmeter.protocol.technetium.samplers;

import me.schiz.jmeter.argentum.reporters.ArgentumListener;
import me.schiz.jmeter.protocol.technetium.callbacks.TcCQL3StatementCallback;
import me.schiz.jmeter.protocol.technetium.config.TcSourceElement;
import me.schiz.jmeter.protocol.technetium.pool.*;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TcCQL3StatementSampler
    extends AbstractSampler
        implements TestBean {

    private static final Logger log = LoggingManager.getLoggerForClass();

    private String source = "TcCQL3StatementSampler.source";
    private String query = "TcCQL3StatementSampler.query";
    private String poolTimeout = "TcCQL3StatementSampler.poolTimeout";
    private String compression = "TcCQL3StatementSampler.compression";
    private String consistencyLevel = "TcCQL3StatementSampler.consistencyLevel";
    private String notifyOnlyArgentums  =   "TcAsyncInsertSampler.notifyOnlyArgentums";


    public static long DEFAULT_POOL_TIMEOUT = 5000; //5ms
    public static String ERROR_RC = "500";
    private static ConcurrentLinkedQueue<SampleResult>  asyncQueue = null;

    public TcCQL3StatementSampler() {
        if(asyncQueue == null) {
            synchronized (TcCQL3StatementSampler.class) {
                if(asyncQueue == null) {
                    asyncQueue = new ConcurrentLinkedQueue<SampleResult>();
                }
            }
        }
    }

    public Long getPoolTimeoutAsLong() {
        return getPropertyAsLong(poolTimeout, DEFAULT_POOL_TIMEOUT);
    }

    public String getPoolTimeout() {
        return getPropertyAsString(poolTimeout, String.valueOf(DEFAULT_POOL_TIMEOUT));
    }

    public void setPoolTimeout(String poolTimeout) {
        setProperty(this.poolTimeout, poolTimeout);
    }

    public String getSource() {
        return getPropertyAsString(source);
    }

    public void setSource(String source) {
        setProperty(this.source, source);
    }

    public String getQuery() {
        return getPropertyAsString(query);
    }

    public void setQuery(String query) {
        setProperty(this.query, query);
    }
    public boolean getNotifyOnlyArgentums() {
        return getPropertyAsBoolean(notifyOnlyArgentums);
    }
    public void setNotifyOnlyArgentums(boolean notifyOnlyArgentums) {
        setProperty(this.notifyOnlyArgentums, notifyOnlyArgentums);
    }

    @Override
    public SampleResult sample(Entry entry) {
        SampleResult asyncResult = null;
        if(!getNotifyOnlyArgentums())   asyncResult = asyncQueue.poll();
        SampleResult newResult = new SampleResult();

        TcInstance tcInstance = null;

        try {
            newResult.setSampleLabel(getName());
            newResult.setSuccessful(false);

            byte[] query_bytes = getQuery().getBytes();
            ByteBuffer queryBB = ByteBuffer.allocate(query_bytes.length);
            queryBB.put(query_bytes);
            queryBB.flip();

            newResult.sampleStart();
            try {
                tcInstance = TcSourceElement.getSource(getSource()).getInstance(getPoolTimeoutAsLong());
            } catch (PoolTimeoutException e) {
                TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                newResult.setResponseData(e.toString().getBytes());
                newResult.setResponseCode(ERROR_RC);
                newResult.setSuccessful(false);
            } catch (TException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (FailureKeySpace failureKeySpace) {
                TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                newResult.setResponseData(failureKeySpace.toString().getBytes());
                newResult.setResponseCode(ERROR_RC);
                newResult.setSuccessful(false);
                tcInstance = null;
            }
            newResult.latencyEnd();
            if(tcInstance != null) {
                try {
                    Cassandra.AsyncClient client = tcInstance.getClient();

                    client.execute_cql3_query(queryBB,
                            Compression.NONE,
                            ConsistencyLevel.THREE,
                            new TcCQL3StatementCallback(newResult,
                                    asyncQueue,
                                    TcSourceElement.getSource(getSource()),
                                    tcInstance,
                                    getNotifyOnlyArgentums())
                    );
                } catch (IllegalStateException ise) {
                    ise.printStackTrace();
                    TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                    newResult.setResponseData(ise.toString().getBytes());
                    newResult.setResponseCode(ERROR_RC);
                    newResult.setSuccessful(false);
                } catch (TException e) {
                    e.printStackTrace();
                    TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                    newResult.setResponseData(e.getMessage().getBytes());
                    newResult.setResponseCode(ERROR_RC);
                    newResult.setSuccessful(false);
                }
            }
        } catch (IOException e) {
            newResult.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
            TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } catch (NotFoundHostException e) {
            newResult.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } catch (InterruptedException e) {
            newResult.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } finally {
            //if(!newResult.isSuccessful())   while(!asyncQueue.add(newResult)){}
            if(getNotifyOnlyArgentums() && !newResult.isSuccessful()) ArgentumListener.sampleOccured(new SampleEvent(newResult, null));
            else    while(!asyncQueue.add(newResult)){}
        }

        return asyncResult;
    }
}
