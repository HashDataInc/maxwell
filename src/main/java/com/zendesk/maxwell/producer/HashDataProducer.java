package com.zendesk.maxwell.producer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.metrics.MaxwellMetrics;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;

public class HashDataProducer extends AbstractProducer {
	private final MaxwellConfig conf;
	private final ArrayBlockingQueue<RowMap> queue;
	private final HashDataProducerWorker worker;

	public HashDataProducer(MaxwellContext context, MaxwellConfig conf) {
		super(context);
		this.conf = conf;
		this.queue = new ArrayBlockingQueue<>(100);
		this.worker = new HashDataProducerWorker(context, this.conf, this.queue);
		Thread thread = new Thread(this.worker, "hashdata-worker");
		thread.setDaemon(true);
		thread.start();
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.queue.put(r);
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this.worker;
	}
}

class HashDataProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
	static final Logger LOGGER = LoggerFactory.getLogger(HashDataProducer.class);

	private final Timer metricsTimer;
	private final ArrayBlockingQueue<RowMap> queue;
	private Thread thread;
	private StoppableTaskState taskState;

	private final Counter succeededMessageCount = MaxwellMetrics.metricRegistry.counter(succeededMessageCountName);
	private final Meter succeededMessageMeter = MaxwellMetrics.metricRegistry.meter(succeededMessageMeterName);
	private final Counter failedMessageCount = MaxwellMetrics.metricRegistry.counter(failedMessageCountName);
	private final Meter failedMessageMeter = MaxwellMetrics.metricRegistry.meter(failedMessageMeterName);

	public HashDataProducerWorker(MaxwellContext context, MaxwellConfig conf, ArrayBlockingQueue<RowMap> queue) {
		super(context);

		this.metricsTimer = MaxwellMetrics.metricRegistry
				.timer(MetricRegistry.name(MaxwellMetrics.getMetricsPrefix(), "time", "overall"));
		this.queue = queue;
		this.taskState = new StoppableTaskState("HashDataProducerWorker");
	}

	@Override
	public void run() {
		this.thread = Thread.currentThread();
		while (true) {
			try {
				RowMap row = queue.take();
				if (!taskState.isRunning()) {
					taskState.stopped();
					return;
				}
				this.push(row);
			} catch (Exception e) {
				taskState.stopped();
				context.terminate(e);
				return;
			}
		}
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		// String key = r.pkToJson();
		String value = r.toJSON(outputConfig);
		System.out.println(value);
		cc.markCompleted();
	}

	@Override
	public void requestStop() {
		taskState.requestStop();
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
		taskState.awaitStop(thread, timeout);
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
}
