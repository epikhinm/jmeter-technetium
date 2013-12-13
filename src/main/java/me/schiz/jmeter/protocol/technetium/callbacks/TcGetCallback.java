package me.schiz.jmeter.protocol.technetium.callbacks;

import me.schiz.jmeter.argentum.reporters.ArgentumListener;
import me.schiz.jmeter.protocol.technetium.HTTPCodes;
import me.schiz.jmeter.protocol.technetium.pool.NetflixUtils;
import me.schiz.jmeter.protocol.technetium.pool.TcInstance;
import me.schiz.jmeter.protocol.technetium.pool.TcPool;
import me.schiz.jmeter.protocol.technetium.samplers.TcCQL3StatementSampler;
import org.apache.cassandra.thrift.*;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TcGetCallback implements AsyncMethodCallback<Cassandra.AsyncClient.get_call> {
    private static final Logger log = LoggingManager.getLoggerForClass();
    private static final String LINE = "========================================";
	private static final String EMPTY_STRING = "(empty)";
	private static final String NULL_STRING = "(null)";
	private static final String SEPARATOR = System.lineSeparator();

    private SampleResult result;
    private ConcurrentLinkedQueue<SampleResult> queue;
    private TcPool pool;
    private int instance_id;
    private boolean notifyOnlyArgentumListeners;

    public TcGetCallback(SampleResult result, ConcurrentLinkedQueue<SampleResult> asyncQueue, TcPool pool, int instance_id, boolean notifyOnlyArgentumListeners) {
        this.result = result;
        this.queue = asyncQueue;
        this.pool = pool;
		this.instance_id = instance_id;
        this.notifyOnlyArgentumListeners = notifyOnlyArgentumListeners;
    }
    @Override
    public void onComplete(Cassandra.AsyncClient.get_call response) {
        try {
            ColumnOrSuperColumn colOrSuperCol = response.getResult();

            if(colOrSuperCol.isSetColumn()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Column name: ");
                sb.append(new String(colOrSuperCol.getColumn().getName(), Charset.defaultCharset()));
                sb.append(SEPARATOR);
                sb.append("Timestamp: ");
                sb.append(colOrSuperCol.getColumn().getTimestamp());
                sb.append(SEPARATOR);
                sb.append("TTL: ");
                sb.append(colOrSuperCol.getColumn().getTtl());
                sb.append(SEPARATOR);
                sb.append("Value: ");
                sb.append(new String(colOrSuperCol.getColumn().getValue()));

                this.result.setResponseData(sb.toString().getBytes(Charset.defaultCharset()));
            }
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
        } 	finally {
            try {
                pool.releaseInstance(instance_id);
            } catch (InterruptedException e) {
                log.warn("cannot release instance. I'll destroy him! ", e);
                pool.destroyInstance(instance_id);
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
        //result.setResponseCode(HTTPCodes.INTERNAL_SERVER_ERROR_500);
        result.setSuccessful(false);

        pool.destroyInstance(instance_id);

        if(notifyOnlyArgentumListeners) {
            ArgentumListener.sampleOccured(new SampleEvent(this.result, null));
        }
        else while(!queue.add(this.result)) {}
    }

}
