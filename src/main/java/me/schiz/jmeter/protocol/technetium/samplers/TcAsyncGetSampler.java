package me.schiz.jmeter.protocol.technetium.samplers;

import me.schiz.jmeter.argentum.reporters.ArgentumListener;
import me.schiz.jmeter.protocol.technetium.HTTPCodes;
import me.schiz.jmeter.protocol.technetium.callbacks.TcGetCallback;
import me.schiz.jmeter.protocol.technetium.config.TcSourceElement;
import me.schiz.jmeter.protocol.technetium.pool.*;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnPath;
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

public class TcAsyncGetSampler
        extends AbstractSampler
        implements TestBean {
    private static final Logger log = LoggingManager.getLoggerForClass();

    private String source               =   "TcAsyncInsertSampler.source";
    private String key                  =   "TcAsyncInsertSampler.key";
    private String keySerializerType    =   "TcAsyncInsertSampler.keySerializerType";
    private String columnParent         =   "TcAsyncInsertSampler.columnParent";
    private String column               =   "TcAsyncInsertSampler.column";
    private String columnSerializerType =   "TcAsyncInsertSampler.columnSerializerType";
//    private String timestamp            =   "TcAsyncInsertSampler.timestamp";
//    private String value                =   "TcAsyncInsertSampler.value";
//    private String valueSerializerType  =   "TcAsyncInsertSampler.valueSerializerType";
    private String poolTimeout          =   "TcAsyncInsertSampler.poolTimeout";
    private String consistencyLevel     =   "TcAsyncInsertSampler.consistencyLevel";
    private String notifyOnlyArgentums  =   "TcAsyncInsertSampler.notifyOnlyArgentums";
//    private String includePoolTime      =   "TcAsyncInsertSampler.includePoolTime";

    public static long DEFAULT_POOL_TIMEOUT = 5000; //5ms
    public static String ERROR_RC = "500";
    private static ConcurrentLinkedQueue<SampleResult> asyncQueue = null;

    public TcAsyncGetSampler() {
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
//    public String getTimestamp() {
//        return getPropertyAsString(timestamp);
//    }
//    public Long getTimestampAsLong() {
//        return getPropertyAsLong(timestamp);
//    }
//    public void setTimestamp(String timestamp) {
//        setProperty(this.timestamp, timestamp);
//    }
//    public String getValue() {
//        return getPropertyAsString(value);
//    }
//    public void setValue(String value) {
//        setProperty(this.value, value);
//    }
    public String getConsistencyLevel() {
        return getPropertyAsString(consistencyLevel);
    }
    public void setConsistencyLevel(String consistencyLevel) {
        setProperty(this.consistencyLevel, consistencyLevel);
    }
    public String getKeySerializerType() {
        return getPropertyAsString(keySerializerType, "AsciiSerializer");
    }
    public void setKeySerializerType(String keySerializerType) {
        setProperty(this.keySerializerType, keySerializerType);
    }
    public String getColumnSerializerType() {
        return getPropertyAsString(columnSerializerType, "AsciiSerializer");
    }
    public void setColumnSerializerType(String columnSerializerType) {
        setProperty(this.columnSerializerType, columnSerializerType);
    }
//    public String getValueSerializerType() {
//        return getPropertyAsString(valueSerializerType, "AsciiSerializer");
//    }
//    public void setValueSerializerType(String valueSerializerType) {
//        setProperty(this.valueSerializerType, valueSerializerType);
//    }
    public boolean getNotifyOnlyArgentums() {
        return getPropertyAsBoolean(notifyOnlyArgentums);
    }
    public void setNotifyOnlyArgentums(boolean notifyOnlyArgentums) {
        setProperty(this.notifyOnlyArgentums, notifyOnlyArgentums);
    }
//    public boolean getIncludePoolTime() {
//        return getPropertyAsBoolean(includePoolTime, false);
//    }
//    public void setIncludePoolTime(boolean includePoolTime) {
//        setProperty(this.includePoolTime, includePoolTime);
//    }

    @Override
    public SampleResult sample(Entry entry) {
        SampleResult asyncResult = null;
        if(!getNotifyOnlyArgentums())   asyncResult = asyncQueue.poll();

        SampleResult newResult = new SampleResult();

        TcInstance tcInstance = null;

        try {

            newResult.setSampleLabel(getName());
            newResult.setSuccessful(true);

            ByteBuffer key = NetflixUtils.serializers.get(getKeySerializerType()).toByteBuffer(
                    NetflixUtils.convert(getKey(), getKeySerializerType())
            );

//            Column column = new Column();
//            if(!getColumn().isEmpty()) {
//                column.setName(NetflixUtils.serializers.get(getColumnSerializerType()).toBytes(
//                        NetflixUtils.convert(getColumn(), getColumnSerializerType())
//                ));
//            }
            ColumnPath columnPath = new ColumnPath();
            columnPath.setColumn_family(getColumnParent());

            if(getColumn() != null && getColumnSerializerType() != null) {
                if(!getColumn().isEmpty() && !getColumnSerializerType().isEmpty()) {
                    columnPath.setColumn(NetflixUtils.serializers.get(getColumnSerializerType()).toBytes(
                            NetflixUtils.convert(getColumn(), getColumnSerializerType())));
                }
            }


//            if(getTimestamp().equalsIgnoreCase("NOW"))  column.setTimestamp(System.currentTimeMillis());
//            else column.setTimestamp(getTimestampAsLong());
//            if(!getValue().isEmpty()) {
//                column.setValue(NetflixUtils.serializers.get(getValueSerializerType()).toBytes(
//                        NetflixUtils.convert(getValue(), getValueSerializerType())
//                ));
//            }

//            if(getIncludePoolTime())    newResult.sampleStart();
            try {
                tcInstance = TcSourceElement.getSource(getSource()).getInstance(getPoolTimeoutAsLong());
//                if(getIncludePoolTime())    newResult.latencyEnd();
            } catch (PoolTimeoutException e) {
//                if(getIncludePoolTime())    newResult.sampleEnd();
                TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                newResult.setResponseData(e.toString().getBytes());
                newResult.setResponseCode(ERROR_RC);
                newResult.setSuccessful(false);
            } catch (TException e) {
//                if(getIncludePoolTime())    newResult.sampleEnd();
                TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                newResult.setResponseData(e.toString().getBytes());
                newResult.setResponseCode(ERROR_RC);
                newResult.setSuccessful(false);
                tcInstance = null;
            } catch (FailureKeySpace failureKeySpace) {
//                if(getIncludePoolTime())    newResult.sampleEnd();
                TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                newResult.setResponseData(failureKeySpace.toString().getBytes());
                newResult.setResponseCode(ERROR_RC);
                newResult.setSuccessful(false);
                tcInstance = null;
            }

            if(tcInstance != null) {
                try {
                    newResult.sampleStart();
                    Cassandra.AsyncClient client = tcInstance.getClient();
                    client.get(key,
                            columnPath,
                            ConsistencyLevel.valueOf(getConsistencyLevel()),
                            new TcGetCallback(newResult, asyncQueue, TcSourceElement.getSource(getSource()), tcInstance, getNotifyOnlyArgentums())
                            );
//                    client.insert(key,
//                            new ColumnParent(!getColumnParent().isEmpty() ? getColumnParent() : ""),
//                            column,
//                            ConsistencyLevel.valueOf(getConsistencyLevel()),
//                            new TcInsertCallback(newResult, asyncQueue, TcSourceElement.getSource(getSource()), tcInstance, getNotifyOnlyArgentums())
//                    );
                    //newResult.latencyEnd();
                } catch (IllegalStateException ise) {
                    newResult.sampleEnd();
                    TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                    newResult.setResponseData(ise.toString().getBytes());
                    newResult.setResponseCode(ERROR_RC);
                    newResult.setSuccessful(false);
                } catch (TException e) {
                    newResult.sampleEnd();
                    TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
                    newResult.setResponseData(e.toString().getBytes());
                    newResult.setResponseCode(ERROR_RC);
                    newResult.setSuccessful(false);
                }
            } else {
                newResult.setResponseCode(HTTPCodes.BAD_REQUEST_400);
                newResult.setSuccessful(false);
            }
        } catch (IOException e) {
            newResult.setResponseData(e.toString().getBytes());
            TcSourceElement.getSource(getSource()).destroyInstance(tcInstance);
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } catch (NotFoundHostException e) {
            newResult.setResponseData(e.toString().getBytes());
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } catch (InterruptedException e) {
            newResult.setResponseData(e.toString().getBytes());
            newResult.setResponseCode(ERROR_RC);
            newResult.setSuccessful(false);
        } finally {
            if(!newResult.isSuccessful()) {
                if(getNotifyOnlyArgentums()) ArgentumListener.sampleOccured(new SampleEvent(newResult, null));
                else    while(!asyncQueue.add(newResult)){}
            }
        }

        return asyncResult;
    }
}
