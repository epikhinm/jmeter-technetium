package me.schiz.jmeter.protocol.technetium.samplers;

import me.schiz.jmeter.protocol.technetium.callbacks.TcInsertCallback;
import me.schiz.jmeter.protocol.technetium.config.TcSourceElement;
import me.schiz.jmeter.protocol.technetium.pool.*;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TcAsyncInsertSampler
        extends AbstractSampler
        implements TestBean {
    private static final Logger log = LoggingManager.getLoggerForClass();

    private String source = "TcAsyncInsertSampler.source";
    private String key = "TcAsyncInsertSampler.key";
    private String keySerializerType = "TcAsyncInsertSampler.keySerializerType";
    private String columnParent = "TcAsyncInsertSampler.columnParent";
    private String column = "TcAsyncInsertSampler.column";
    private String columnSerializerType = "TcAsyncInsertSampler.columnSerializerType";
    private String timestamp = "TcAsyncInsertSampler.timestamp";
    private String value = "TcAsyncInsertSampler.value";
    private String valueSerializerType =  "TcAsyncInsertSampler.valueSerializerType";

    private String poolTimeout = "TcAsyncInsertSampler.poolTimeout";
    private String consistencyLevel = "TcAsyncInsertSampler.consistencyLevel";


    public static long DEFAULT_POOL_TIMEOUT = 5000; //5ms
    public static String ERROR_RC = "500";
    private static ConcurrentLinkedQueue<SampleResult> asyncQueue = null;

    public TcAsyncInsertSampler() {
        if(asyncQueue == null) {
            synchronized (TcAsyncInsertSampler.class) {
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
    public String getKey() {
        return getPropertyAsString(key);
    }
    public void setKey(String key) {
        setProperty(this.key, key);
    }
    public String getColumnParent() {
        return getPropertyAsString(columnParent);
    }
    public void setColumnParent(String columnParent) {
        setProperty(this.columnParent, columnParent);
    }
    public String getColumn() {
        return getPropertyAsString(column);
    }
    public void setColumn(String column) {
        setProperty(this.column, column);
    }
    public String getTimestamp() {
        return getPropertyAsString(timestamp);
    }
    public Long getTimestampAsLong() {
        return getPropertyAsLong(timestamp);
    }
    public void setTimestamp(String timestamp) {
        setProperty(this.timestamp, timestamp);
    }
    public String getValue() {
        return getPropertyAsString(value);
    }
    public void setValue(String value) {
        setProperty(this.value, value);
    }
    public String getConsistencyLevel() {
        return getPropertyAsString(consistencyLevel);
    }
    public void setConsistencyLevel(String consistencyLevel) {
        setProperty(this.consistencyLevel, consistencyLevel);
    }
    public String getKeySerializerType() {
        return getPropertyAsString(keySerializerType);
    }
    public void setKeySerializerType(String keySerializerType) {
        setProperty(this.keySerializerType, keySerializerType);
    }
    public String getColumnSerializerType() {
        return getPropertyAsString(columnSerializerType);
    }
    public void setColumnSerializerType(String columnSerializerType) {
        setProperty(this.columnSerializerType, columnSerializerType);
    }
    public String getValueSerializerType() {
        return getPropertyAsString(valueSerializerType);
    }
    public void seValueSerializerType(String valueSerializerType) {
        setProperty(this.valueSerializerType, valueSerializerType);
    }

    @Override
    public SampleResult sample(Entry entry) {
        SampleResult asyncResult = asyncQueue.poll();
        SampleResult newResult = new SampleResult();

        TcInstance tcInstance = null;

        try {
            newResult.setSampleLabel(getName());
            newResult.setSuccessful(false);

            ByteBuffer key = NetflixSerializer.serializers.get(getKeySerializerType()).toByteBuffer(
                    NetflixSerializer.convert(getKey(), getKeySerializerType())
            );

            Column column = new Column();
            if(!getColumn().isEmpty()) {
                column.setValue(NetflixSerializer.serializers.get(getColumnSerializerType()).toByteBuffer(
                        NetflixSerializer.convert(getColumn(), getColumnSerializerType())
                ));
            }
            if(getTimestamp().equalsIgnoreCase("NOW"))  column.setTimestamp(System.currentTimeMillis());
            else column.setTimestamp(getTimestampAsLong());
            if(!getValue().isEmpty()) {
                column.setValue(NetflixSerializer.serializers.get(getValueSerializerType()).toByteBuffer(
                        NetflixSerializer.convert(getValue(), getValueSerializerType())
                ));
            }

            newResult.sampleStart();
            try {
                tcInstance = TcSourceElement.getSource(getSource()).getInstance(getPoolTimeoutAsLong());
            } catch (PoolTimeoutException e) {
                TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                newResult.setResponseData(e.toString().getBytes());
                newResult.setResponseCode(ERROR_RC);
                newResult.setSuccessful(false);
            } catch (TException e) {
                TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                newResult.setResponseData(e.toString().getBytes());
                newResult.setResponseCode(ERROR_RC);
                newResult.setSuccessful(false);
                tcInstance = null;
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
                    client.insert(key,
                            new ColumnParent(!getColumnParent().isEmpty() ? getColumnParent() : ""),
                            column,
                            ConsistencyLevel.valueOf(getConsistencyLevel()),
                                new TcInsertCallback(newResult, asyncQueue, TcSourceElement.getSource(getSource()), tcInstance)
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
            e.printStackTrace();
            TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
            newResult.setResponseData(e.getMessage().getBytes());
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } catch (NotFoundHostException e) {
            e.printStackTrace();
            newResult.setResponseData(e.getMessage().getBytes());
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } catch (InterruptedException e) {
            e.printStackTrace();
            newResult.setResponseData(e.getMessage().getBytes());
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } finally {
            if(!newResult.isSuccessful())   while(!asyncQueue.add(newResult)){}
        }

        return asyncResult;
    }
}
