package me.schiz.jmeter.protocol.technetium.callbacks;

import me.schiz.jmeter.argentum.reporters.ArgentumListener;
import me.schiz.jmeter.protocol.technetium.pool.NetflixUtils;
import me.schiz.jmeter.protocol.technetium.pool.TcPool;
import me.schiz.jmeter.protocol.technetium.samplers.TcCQL3StatementSampler;
import org.apache.cassandra.thrift.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TcCQL3StatementCallback implements AsyncMethodCallback<Cassandra.AsyncClient.execute_cql3_query_call> {
    private static final Logger log = LoggingManager.getLoggerForClass();
    private static final String LINE = "========================================";

    private SampleResult result;
    private ConcurrentLinkedQueue<SampleResult> queue;
    private TcPool pool;
    private int instance_id;
    private boolean notifyOnlyArgentumListeners;

	private static final String EMPTY_STRING = "(empty)";
	private static final String NULL_STRING = "(null)";
	private static final String SEPARATOR = System.lineSeparator();
	private ByteBuffer query;

    public TcCQL3StatementCallback(SampleResult result,
								   ConcurrentLinkedQueue<SampleResult> asyncQueue,
								   TcPool pool,
								   int instance_id,
								   boolean notifyOnlyArgentumListeners,
								   ByteBuffer query) {
        this.result = result;
        this.queue = asyncQueue;
        this.pool = pool;
        this.instance_id = instance_id;
        this.notifyOnlyArgentumListeners = notifyOnlyArgentumListeners;
		this.query = query;
    }
    @Override
    public void onComplete(Cassandra.AsyncClient.execute_cql3_query_call response) {
		try {
			this.result.sampleEnd();

			try {
				CqlResult cqlResult = response.getResult();
				StringBuilder _response = new StringBuilder();
				List<CqlRow> rows= cqlResult.getRows();
				if(rows != null) {
					if(rows.size() == 0) {
						_response.append(EMPTY_STRING);
					}
					for(CqlRow row : cqlResult.getRows()) {
						_response.append(LINE);
						_response.append(SEPARATOR);
						_response.append("key: ");
						_response.append(new String(row.getKey()));
						_response.append(SEPARATOR);
						for(Column col : row.getColumns()) {
							if(col.isSetValue() && col.isSetName())	{

								_response.append(new String(col.getName()));
								_response.append(" : ");
								_response.append(new String(Hex.encodeHex(col.getValue())));
								//_response.append(new String(col.getValue()));
								_response.append(SEPARATOR);
							}
						}
						_response.append(SEPARATOR);
					}
				} else _response.append(NULL_STRING);
				this.result.setResponseData(_response.toString().getBytes());
//				this.result.setResponseData(cqlResult.toString().getBytes(Charset.defaultCharset()));
				this.result.setSuccessful(true);
				this.result.setResponseCodeOK();
			} catch (InvalidRequestException e) {
				this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
				this.result.setSuccessful(false);
				log.error("<" + instance_id + "> " +  NetflixUtils.getStackTrace(e));
			} catch (UnavailableException e) {
				this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
				this.result.setSuccessful(false);
				log.error("<" + instance_id + "> " +  NetflixUtils.getStackTrace(e));
			} catch (TimedOutException e) {
				this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
				this.result.setSuccessful(false);
				log.error("<" + instance_id + "> " +  NetflixUtils.getStackTrace(e));
			} catch (SchemaDisagreementException e) {
				this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
				this.result.setSuccessful(false);
				log.error("<" + instance_id + "> " +  NetflixUtils.getStackTrace(e));
			} catch (TException e) {
				this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
				this.result.setSuccessful(false);
				log.error("<" + instance_id + "> " +  NetflixUtils.getStackTrace(e));
			} finally {
				if(this.result.isResponseCodeOK()) {
					try {
						pool.releaseInstance(instance_id);
					} catch (InterruptedException e) {
						log.warn("cannot release instance. I'll destroy him! ", e);
						pool.destroyInstance(instance_id);
					}
				} else {
					pool.destroyInstance(instance_id);
					log.error("<" + instance_id + "> died");
				}
				if(notifyOnlyArgentumListeners) ArgentumListener.sampleOccured(new SampleEvent(this.result, null));
				else while(!queue.add(this.result)) {}
				query.clear();
			}
		} catch (Exception e) {
			log.warn("onComplete exc", e);
		}
    }

    @Override
    public void onError(Exception e) {
		try{
			this.result.sampleEnd();
			this.result.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
			this.result.setResponseCode(TcCQL3StatementSampler.ERROR_RC);
			this.result.setSuccessful(false);
			log.error("<" + instance_id + "> " + NetflixUtils.getStackTrace(e));
			pool.destroyInstance(instance_id);

			while(!queue.add(this.result)) {}
			log.warn("inner onError exception", e);
		} catch (Exception e1) {
			log.warn("onError exception" ,e1);
		} finally {
			query.clear();
		}
    }
}
