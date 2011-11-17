package com.yammer.metrics.reporting;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.MeterMetric;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.util.MetricPredicate;

public class GraphiteReporterTests
{
    private static GraphiteReporter getMockGraphiteReporter(final OutputStream outputStream, MetricsRegistry metricsRegistry)
    {
        GraphiteReporter graphiteReporter = new GraphiteReporter(metricsRegistry, "prefix", MetricPredicate.ALL, new SocketProvider()
        {

            @Override
            public Socket get() throws Exception
            {
                Socket socket = mock(Socket.class);

                when(socket.getOutputStream()).thenReturn(outputStream);

                return socket;
            }
        });

        graphiteReporter.printVMMetrics = false;

        return graphiteReporter;
    }

    @Test
    public void canRenderCounter()
    {
        StringBuilder expected = new StringBuilder();

        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.count 11 0\n");

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        CounterMetric metric = metricsRegistry.newCounter(getClass(), "test");
        metric.inc(11);

        OutputStream outputStream = new ByteArrayOutputStream();

        getMockGraphiteReporter(outputStream, metricsRegistry).runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRenderCustomCounter()
    {
        String expected = "counter.test.11";

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        CounterMetric metric = metricsRegistry.newCounter(getClass(), "test");
        metric.inc(11);

        OutputStream outputStream = new ByteArrayOutputStream();

        GraphiteReporter reporter = getMockGraphiteReporter(outputStream, metricsRegistry);

        reporter.registerRenderer(CounterMetric.class, new MetricRenderer<CounterMetric>()
        {
            @Override
            public String getData(CounterMetric metric, GraphiteReporterAttributes attributes)
            {
                return "counter.test." + metric.count();
            }
        });

        reporter.runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRenderHistogram()
    {
        StringBuilder expected = new StringBuilder();

        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.min 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.max 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.mean 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.stddev 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.median 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.75percentile 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.95percentile 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.98percentile 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.99percentile 10.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.999percentile 10.00 0\n");

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        HistogramMetric metric = metricsRegistry.newHistogram(getClass(), "test");
        metric.update(10);

        OutputStream outputStream = new ByteArrayOutputStream();

        getMockGraphiteReporter(outputStream, metricsRegistry).runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRenderCustomHistogram()
    {
        String expected = "histogram.test.1";

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        HistogramMetric metric = metricsRegistry.newHistogram(getClass(), "test");
        metric.update(10);

        OutputStream outputStream = new ByteArrayOutputStream();

        GraphiteReporter reporter = getMockGraphiteReporter(outputStream, metricsRegistry);

        reporter.registerRenderer(HistogramMetric.class, new MetricRenderer<HistogramMetric>()
        {
            @Override
            public String getData(HistogramMetric metric, GraphiteReporterAttributes attributes)
            {
                return "histogram.test." + metric.count();
            }
        });

        reporter.runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRendererCustomTimed()
    {
        StringBuilder expected = new StringBuilder();

        expected.append("metered.test.1\n");
        expected.append("timer.test.1\n");

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        TimerMetric metric = metricsRegistry.newTimer(getClass(), "test", "testevent");
        metric.update(12, TimeUnit.MILLISECONDS);

        OutputStream outputStream = new ByteArrayOutputStream();

        GraphiteReporter reporter = getMockGraphiteReporter(outputStream, metricsRegistry);

        reporter.registerRenderer(MeterMetric.class, new MetricRenderer<Metered>()
        {
            @Override
            public String getData(Metered metric, GraphiteReporterAttributes attributes)
            {
                return "metered.test." + metric.count() + "\n";
            }
        });
        reporter.registerRenderer(TimerMetric.class, new MetricRenderer<TimerMetric>()
        {

            @Override
            public String getData(TimerMetric metric, GraphiteReporterAttributes attributes)
            {
                return "timer.test." + metric.count() + "\n";
            }
        });

        reporter.runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRendererTimed()
    {
        StringBuilder expected = new StringBuilder();

        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.count 0 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.meanRate 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.1MinuteRate 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.5MinuteRate 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.15MinuteRate 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.min 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.max 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.mean 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.stddev 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.median 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.75percentile 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.95percentile 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.98percentile 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.99percentile 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.testevent.test.999percentile 0.00 0\n");

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        metricsRegistry.newTimer(getClass(), "test", "testevent");

        OutputStream outputStream = new ByteArrayOutputStream();

        getMockGraphiteReporter(outputStream, metricsRegistry).runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRendererMetered()
    {
        StringBuilder expected = new StringBuilder();

        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.count 0 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.meanRate 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.1MinuteRate 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.5MinuteRate 0.00 0\n");
        expected.append("prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.15MinuteRate 0.00 0\n");

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        metricsRegistry.newMeter(getClass(), "test", "testevent", TimeUnit.SECONDS);

        OutputStream outputStream = new ByteArrayOutputStream();

        getMockGraphiteReporter(outputStream, metricsRegistry).runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRendererCustomMetered()
    {
        String expected = "metered.test.12";

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        MeterMetric metric = metricsRegistry.newMeter(getClass(), "test", "testevent", TimeUnit.SECONDS);
        metric.mark(12);

        OutputStream outputStream = new ByteArrayOutputStream();

        GraphiteReporter reporter = getMockGraphiteReporter(outputStream, metricsRegistry);

        reporter.registerRenderer(MeterMetric.class, new MetricRenderer<Metered>()
        {
            @Override
            public String getData(Metered metric, GraphiteReporterAttributes attributes)
            {
                return "metered.test." + metric.count();
            }
        });

        reporter.runInternal(0);

        assertEquals(expected.toString(), outputStream.toString());
    }

    @Test
    public void canRendererGauge()
    {
        String expected = "prefix.com.yammer.metrics.reporting.GraphiteReporterTests.test.value 5 0\n";

        MetricsRegistry metricsRegistry = new MetricsRegistry();

        metricsRegistry.newGauge(getClass(), "test", new GaugeMetric<Long>()
        {

            @Override
            public void renderMetric(RenderableReporter reporter, RenderAttributes attributes) throws IOException
            {
                reporter.renderGauge(this, attributes);
            }

            @Override
            public Long value()
            {
                return 5l;
            }
        });

        OutputStream outputStream = new ByteArrayOutputStream();

        getMockGraphiteReporter(outputStream, metricsRegistry).runInternal(0);

        assertEquals(expected, outputStream.toString());
    }

    @Test
    public void canRenderCustomGauge()
    {
        String expected = "gauge.test.5";

        MetricsRegistry metricsRegistry = new MetricsRegistry();
        metricsRegistry.newGauge(getClass(), "test", new GaugeMetric<Long>()
        {

            @Override
            public void renderMetric(RenderableReporter reporter, RenderAttributes attributes) throws IOException
            {
                reporter.renderGauge(this, attributes);
            }

            @Override
            public Long value()
            {
                return 5l;
            }
        });

        OutputStream outputStream = new ByteArrayOutputStream();

        GraphiteReporter reporter = getMockGraphiteReporter(outputStream, metricsRegistry);

        reporter.registerRenderer(GaugeMetric.class, new MetricRenderer<GaugeMetric>()
        {

            @Override
            public String getData(GaugeMetric metric, GraphiteReporterAttributes attributes)
            {
                return "gauge.test." + metric.value().toString();
            }
        });

        reporter.runInternal(0);

        assertEquals(expected, outputStream.toString());
    }
}
