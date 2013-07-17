package me.schiz.jmeter.protocol.technetium.callbacks;

import me.schiz.jmeter.protocol.technetium.pool.TcInstance;
import me.schiz.jmeter.protocol.technetium.pool.TcPool;
import me.schiz.jmeter.protocol.technetium.samplers.TcCQL3StatementSampler;
import org.apache.cassandra.thrift.*;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TcCQL3StatementCallback implements AsyncMethodCallback<Cassandra.AsyncClient.execute_cql3_query_call> {
    private static final Logger log = LoggingManager.getLoggerForClass();
    private static String separator = "========================================";

    private SampleResult result;
    private ConcurrentLinkedQueue<SampleResult> queue;
    private TcPool pool;
    private TcInstance instance;

    public TcCQL3StatementCallback(SampleResult result, ConcurrentLinkedQueue<SampleResult> asyncQueue, TcPool pool, TcInstance instance) {
        this.result = result;
        this.queue = asyncQueue;
        this.pool = pool;
        this.instance = instance;
    }
    @Override
    public void onComplete(Cassandra.AsyncClient.execute_cql3_query_call response) {
        this.result.sampleEnd();

        try {
            CqlResult cqlResult = response.getResult();
            String _response  = new String();
            List<CqlRow> rows= cqlResult.getRows();
            if(rows != null) {
                for(CqlRow row : cqlResult.getRows()) {
                    _response += separator + "\n";
                    _response += "key: " + new String(row.getKey()) + "\n";
                    for(Column col : row.getColumns()) {
                        _response += new String(col.getName()) + " : " + new String(col.getValue()) + "\n";
                    }
                    _response += "\n";
                }
            }
            this.result.setResponseData(_response.getBytes());
            this.result.setSuccessful(true);
        } catch (InvalidRequestException e) {
            e.printStackTrace();
            this.result.setResponseData(e.toString().getBytes());
            this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
            this.result.setSuccessful(false);
        } catch (UnavailableException e) {
            e.printStackTrace();
            this.result.setResponseData(e.toString().getBytes());
            this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
            this.result.setSuccessful(false);
        } catch (TimedOutException e) {
            e.printStackTrace();
            this.result.setResponseData(e.toString().getBytes());
            this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
            this.result.setSuccessful(false);
        } catch (SchemaDisagreementException e) {
            e.printStackTrace();
            this.result.setResponseData(e.toString().getBytes());
            this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
            this.result.setSuccessful(false);
        } catch (TException e) {
            e.printStackTrace();
            this.result.setResponseData(e.toString().getBytes());
            this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
            this.result.setSuccessful(false);
        } finally {
            try {
                pool.releaseInstance(instance);
            } catch (InterruptedException e) {
                log.warn("cannot release instance. I'll destroy him! ", e);
                pool.destroyInstance(instance);
            }
            while(!queue.add(this.result)) {}
        }
    }

    @Override
    public void onError(Exception e) {
        result.setResponseData(e.toString().getBytes());
        result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
        result.setSuccessful(false);

        try {
            pool.releaseInstance(instance);
        } catch (InterruptedException ie) {
            log.warn("cannot release instance. I'll destroy him! ", ie);
            pool.destroyInstance(instance);
        }

        while(!queue.add(result)) {}
    }
}
