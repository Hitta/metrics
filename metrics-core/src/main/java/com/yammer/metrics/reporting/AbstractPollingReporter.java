package com.yammer.metrics.reporting;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.util.NamedThreadFactory;

public abstract class AbstractPollingReporter extends AbstractReporter implements Runnable, RenderableReporter {
    private final ScheduledExecutorService executor;

    protected AbstractPollingReporter(MetricsRegistry registry, String name) {
        super(registry);
        this.executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(name));
    }

    public void start(long pollingTime, TimeUnit pollingTimeUnit) {
        executor.scheduleWithFixedDelay(this, pollingTime, pollingTime, pollingTimeUnit);
    }

    public void shutdown(long waitTime, TimeUnit waitTimeMillis) throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(waitTime, waitTimeMillis);
    }

    public void shutdown() {
        executor.shutdown();
    }
}
