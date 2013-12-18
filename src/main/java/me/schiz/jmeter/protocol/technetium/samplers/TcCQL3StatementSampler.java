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
    extends AbstractSampler
        implements TestBean {

    private static final Logger log = LoggingManager.getLoggerForClass();

    public static final String source = "TcCQL3StatementSampler.source";
    public static final String query = "TcCQL3StatementSampler.query";
    public static String compression = "TcCQL3StatementSampler.compression";
    public static final String consistencyLevel = "TcCQL3StatementSampler.consistencyLevel";
    public static final String notifyOnlyArgentums  =   "TcAsyncInsertSampler.notifyOnlyArgentums";

	public static final long MAX_ONLINE_TASKS = 64;
    public static final String ERROR_RC = "500";
	private ThreadLocal<ConcurrentLinkedQueue<SampleResult>>	tlAsyncQueue = new ThreadLocal<ConcurrentLinkedQueue<SampleResult>>();
	private ThreadLocal<AtomicInteger> tlOnlineCounter = new ThreadLocal<AtomicInteger>();

    public TcCQL3StatementSampler() {
		if(tlAsyncQueue.get() == null) tlAsyncQueue.set(new ConcurrentLinkedQueue<SampleResult>());
		if(tlOnlineCounter.get() == null)	tlOnlineCounter.set(new AtomicInteger(0));
    }

	public String getConsistencyLevel()
	{
		return getPropertyAsString(consistencyLevel, "ALL");
	}
	public void setConsistencyLevel(String CL) {
		setProperty(consistencyLevel, CL);
	}
	private ConsistencyLevel consistency() {
		return ConsistencyLevel.valueOf(getConsistencyLevel().toUpperCase());
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
        SampleResult asyncResult = null;
        if(!getNotifyOnlyArgentums()) {
			asyncResult = tlAsyncQueue.get().poll();
			if(asyncResult != null)	onlineCounter.decrementAndGet();
		}
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
					tcInstance = null;
				} catch (FailureKeySpace failureKeySpace) {
					TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
					newResult.setResponseData(failureKeySpace.toString().getBytes());
					newResult.setResponseCode(ERROR_RC);
					tcInstance = null;
				}
				newResult.sampleStart();
				if(tcInstance != null) {
					try {
						Cassandra.AsyncClient client = tcInstance.getClient();
		                newResult.setSuccessful(true);
						client.execute_cql3_query(queryBB,
								Compression.NONE,
								consistency(),
								new TcCQL3StatementCallback(newResult,
										tlAsyncQueue.get(),
										TcSourceElement.getSource(getSource()),
										instance_id,
										getNotifyOnlyArgentums(),
										queryBB));
					} catch (IllegalStateException ise) {
						TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
						newResult.setResponseData(ise.toString().getBytes());
						newResult.setResponseCode(ERROR_RC);
						newResult.setSuccessful(false);
						queryBB.clear();
					} catch (TException e) {
						TcSourceElement.getSource(getSource()).destroyInstance(instance_id);
						newResult.setResponseData(e.getMessage().getBytes());
						newResult.setSuccessful(false);
						newResult.setResponseCode(ERROR_RC);
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
			} catch (NotFoundHostException e) {
				newResult.setResponseData(NetflixUtils.getStackTrace(e).getBytes());
				newResult.setResponseCode(ERROR_RC);
			} finally {
				if(!newResult.isSuccessful()) {
					if(getNotifyOnlyArgentums() && !newResult.isSuccessful()) ArgentumListener.sampleOccured(new SampleEvent(newResult, null));
					else    while(!tlAsyncQueue.get().add(newResult)){}
				}
				onlineCounter.incrementAndGet();
			}
		}
        return asyncResult;
    }
}
