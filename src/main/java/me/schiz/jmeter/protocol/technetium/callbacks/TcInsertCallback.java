package me.schiz.jmeter.protocol.technetium.callbacks;

import me.schiz.jmeter.argentum.reporters.ArgentumListener;
import me.schiz.jmeter.protocol.technetium.HTTPCodes;
import me.schiz.jmeter.protocol.technetium.pool.NetflixUtils;
import me.schiz.jmeter.protocol.technetium.pool.TcInstance;
import me.schiz.jmeter.protocol.technetium.pool.TcPool;
import me.schiz.jmeter.protocol.technetium.samplers.TcCQL3StatementSampler;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.ConcurrentLinkedQueue;

public class TcInsertCallback implements AsyncMethodCallback<Cassandra.AsyncClient.insert_call> {
    private static final Logger log = LoggingManager.getLoggerForClass();
    private static String separator = "========================================";

    private SampleResult result;
    private ConcurrentLinkedQueue<SampleResult> queue;
    private TcPool pool;
    private TcInstance instance;
    private boolean notifyOnlyArgentumListeners;

    public TcInsertCallback(SampleResult result, ConcurrentLinkedQueue<SampleResult> asyncQueue, TcPool pool, TcInstance instance, boolean notifyOnlyArgentumListeners) {
        this.result = result;
        this.queue = asyncQueue;
        this.pool = pool;
        this.instance = instance;
        this.notifyOnlyArgentumListeners = notifyOnlyArgentumListeners;
    }
    @Override
    public void onComplete(Cassandra.AsyncClient.insert_call response) {
        try {
            response.getResult();
            if(result.getEndTime() == 0)    result.sampleEnd();
            this.result.setSuccessful(true);
        } catch (InvalidRequestException e) {
            this.result.sampleEnd();
            this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
            this.result.setResponseCode(HTTPCodes.BAD_REQUEST_400);
            this.result.setSuccessful(false);
        } catch (UnavailableException e) {
            this.result.sampleEnd();
            this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());;
            this.result.setResponseCode(HTTPCodes.INTERNAL_SERVER_ERROR_500);
            this.result.setSuccessful(false);
        } catch (TimedOutException e) {
            this.result.sampleEnd();
            this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
            this.result.setResponseCode(HTTPCodes.REQUEST_TIMEOUT_408);
            this.result.setSuccessful(false);
        } catch (TException e) {
            this.result.sampleEnd();
            this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
            this.result.setResponseCode(HTTPCodes.INTERNAL_SERVER_ERROR_500);
            this.result.setSuccessful(false);
        } finally {
            try {
                pool.releaseInstance(instance);
            } catch (InterruptedException e) {
                log.warn("cannot release instance. I'll destroy him! ", e);
                pool.destroyInstance(instance);
            }
            if(notifyOnlyArgentumListeners) ArgentumListener.sampleOccured(new SampleEvent(this.result, null));
            else while(!queue.add(this.result)) {}
        }
    }

    @Override
    public void onError(Exception e) {
        if(result.getEndTime() == 0)    result.sampleEnd();
        result.setResponseData(e.toString().getBytes());
        result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
        result.setResponseCode(HTTPCodes.INTERNAL_SERVER_ERROR_500);
        result.setSuccessful(false);

        //always destroy bad instance
        pool.destroyInstance(instance);

        try {
            pool.releaseInstance(instance);
        } catch (InterruptedException ie) {
            log.warn("cannot release instance. I'll destroy him! ", ie);
            pool.destroyInstance(instance);
        }

        if(notifyOnlyArgentumListeners) {
            ArgentumListener.sampleOccured(new SampleEvent(this.result, null));
        }
        else while(!queue.add(this.result)) {}
    }

}
