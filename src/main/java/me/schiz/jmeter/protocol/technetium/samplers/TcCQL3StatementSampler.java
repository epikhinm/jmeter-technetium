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
import java.util.concurrent.atomic.AtomicInteger;

public class TcCQL3StatementSampler
    extends AbstractSampler {

    private static final Logger log = LoggingManager.getLoggerForClass();

    public static final String SOURCE = "TcCQL3StatementSampler.source";
    public static final String QUERY = "TcCQL3StatementSampler.query";
    public static final String COMPRESSION = "TcCQL3StatementSampler.compression";
	public static final String TIMEOUT = "TcCQL3StatementSampler.timeout";
    public static final String CONSISTENCY = "TcCQL3StatementSampler.consistencyLevel";
	public static final String NOTIFY_ONLY_ARGENTUMS  =   "TcAsyncInsertSampler.notifyOnlyArgentums";

	public static final String DEFAULT_CONSISTENCY = "ALL";
	public static final String DEFAULT_COMPRESSION = "NONE";
	public static final String DEFAULT_TIMEOUT = "1000";

	public static final long MAX_ONLINE_TASKS = 64;
    public static final String ERROR_RC = "500";
	private ThreadLocal<ConcurrentLinkedQueue<SampleResult>>	tlAsyncQueue = new ThreadLocal<ConcurrentLinkedQueue<SampleResult>>();
	private ThreadLocal<AtomicInteger> tlOnlineCounter = new ThreadLocal<AtomicInteger>();

    public TcCQL3StatementSampler() {
		if(tlAsyncQueue.get() == null) tlAsyncQueue.set(new ConcurrentLinkedQueue<SampleResult>());
		if(tlOnlineCounter.get() == null)	tlOnlineCounter.set(new AtomicInteger(0));
    }

	public String getConsistency()
	{
		return getPropertyAsString(CONSISTENCY);
	}
	public void setConsistency(String CL) {
		setProperty(CONSISTENCY, CL);
	}
	private ConsistencyLevel consistency() {
		return ConsistencyLevel.valueOf(getConsistency());
	}

    public String getSource() {
        return getPropertyAsString(SOURCE);
    }

    public void setSource(String source) {
        setProperty(SOURCE, source);
    }

    public String getQuery() {
        return getPropertyAsString(QUERY);
    }

    public void setQuery(String query) {
        setProperty(QUERY, query);
    }
    public boolean getNotifyOnlyArgentums() {
        return getPropertyAsBoolean(NOTIFY_ONLY_ARGENTUMS);
    }
    public void setNotifyOnlyArgentums(boolean notifyOnlyArgentums) {
        setProperty(NOTIFY_ONLY_ARGENTUMS, notifyOnlyArgentums);
    }

	public String getCompression() {
		return getPropertyAsString(COMPRESSION);
	}

	public void setCompression(String compression) {
		setProperty(COMPRESSION, compression);
	}

	public long getTimeout(){
		return getPropertyAsLong(TIMEOUT);
	}

	public void setTimeout(String timeout) {
		setProperty(TIMEOUT, timeout);
	}

	private void doRequest(ConcurrentLinkedQueue<SampleResult> asyncQueue, AtomicInteger onlineCounter) {
		SampleResult newResult = new SampleResult();
		int instance_id = -1;
		TcInstance tcInstance;

		if(onlineCounter.get() < MAX_ONLINE_TASKS) {
			try {
				newResult.setSampleLabel(getName());
				newResult.setSuccessful(false);
				newResult.setRequestHeaders(getQuery());

				byte[] query_bytes = getQuery().getBytes();
				ByteBuffer queryBB = ByteBuffer.allocate(query_bytes.length);
				queryBB.put(query_bytes);
				queryBB.flip();
				try {
					instance_id = TcSourceElement.getSource(getSource()).getFreeInstanceId();
					tcInstance = TcSourceElement.getSource(getSource()).getInstance(instance_id);
				} catch (TException e) {
					TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
					newResult.setResponseData(e.toString().getBytes());
					newResult.setResponseCode(ERROR_RC);
					log.error("<" + instance_id + ">" + NetflixUtils.getStackTrace(e));
					tcInstance = null;
				} catch (FailureKeySpace failureKeySpace) {
					TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
					newResult.setResponseData(failureKeySpace.toString().getBytes());
					newResult.setResponseCode(ERROR_RC);
					log.error("<" + instance_id + ">" + NetflixUtils.getStackTrace(failureKeySpace));
					tcInstance = null;
				}
				newResult.sampleStart();
				if(tcInstance != null) {
					try {
						Cassandra.AsyncClient client = tcInstance.getClient();
						newResult.setSuccessful(true);
						//long to = getTimeout();
						//if(client.getTimeout() != to)	client.setTimeout(to);
						client.execute_cql3_query(queryBB,
								//Compression.valueOf(getCompression()),
								Compression.NONE,
								consistency(),
								new TcCQL3StatementCallback(newResult,
										asyncQueue,
										TcSourceElement.getSource(getSource()),
										instance_id,
										getNotifyOnlyArgentums(),
										queryBB));
					} catch (IllegalStateException ise) {
						TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
						newResult.setResponseData(ise.toString().getBytes());
						newResult.setResponseCode(ERROR_RC);
						newResult.setSuccessful(false);
						log.error("<" + instance_id + ">" + NetflixUtils.getStackTrace(ise));
						queryBB.clear();
					} catch (TException e) {
						TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
						newResult.setResponseData(e.getMessage().getBytes());
						newResult.setSuccessful(false);
						newResult.setResponseCode(ERROR_RC);
						log.error("<" + instance_id + ">" + NetflixUtils.getStackTrace(e));
						queryBB.clear();
					}
				} else {
					newResult.setResponseCode(ERROR_RC);
					newResult.setResponseData("instance is null".getBytes());
					log.error("null_instance");
				}
			} catch (IOException e) {
				newResult.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
				newResult.setResponseCode(ERROR_RC);
				log.error("<" + instance_id + ">" + NetflixUtils.getStackTrace(e));
			} catch (NotFoundHostException e) {
				newResult.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				newResult.setResponseCode(ERROR_RC);
				log.error("<" + instance_id + ">" + NetflixUtils.getStackTrace(e));
			} finally {
				if(!newResult.isSuccessful()) {
					if(getNotifyOnlyArgentums() && !newResult.isSuccessful()) ArgentumListener.sampleOccured(new SampleEvent(newResult, null));
					else    while(!asyncQueue.add(newResult)){}
				}
				onlineCounter.incrementAndGet();
			}
		}

	}

    @Override
    public SampleResult sample(Entry entry) {
		AtomicInteger onlineCounter = tlOnlineCounter.get();
		ConcurrentLinkedQueue<SampleResult> queue = tlAsyncQueue.get();
		if(queue == null) 	{
			queue = new ConcurrentLinkedQueue<SampleResult>();
			tlAsyncQueue.set(queue);
		}
		if(onlineCounter == null) {
			log.error("counter is null");
			onlineCounter = new AtomicInteger(0);
			tlOnlineCounter.set(onlineCounter);
		}
		doRequest(queue, onlineCounter);
        SampleResult asyncResult = null;
        if(!getNotifyOnlyArgentums()) {
			asyncResult = tlAsyncQueue.get().poll();
			if(asyncResult != null)	onlineCounter.decrementAndGet();
		}
        return asyncResult;
    }
}
