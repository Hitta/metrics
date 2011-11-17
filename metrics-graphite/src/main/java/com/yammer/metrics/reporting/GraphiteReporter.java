package com.yammer.metrics.reporting;

import static com.yammer.metrics.core.VirtualMachineMetrics.daemonThreadCount;
import static com.yammer.metrics.core.VirtualMachineMetrics.fileDescriptorUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.garbageCollectors;
import static com.yammer.metrics.core.VirtualMachineMetrics.heapUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.memoryPoolUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.nonHeapUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.threadCount;
import static com.yammer.metrics.core.VirtualMachineMetrics.threadStatePercentages;
import static com.yammer.metrics.core.VirtualMachineMetrics.uptime;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.Thread.State;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.MeterMetric;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.core.VirtualMachineMetrics.GarbageCollector;
import com.yammer.metrics.util.MetricPredicate;
import com.yammer.metrics.util.Utils;

/**
 * A simple reporter which sends out application metrics to a
 * <a href="http://graphite.wikidot.com/faq">Graphite</a> server periodically.
 */
public class GraphiteReporter extends AbstractPollingReporter
{
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteReporter.class);
    private final String prefix;
    private final MetricPredicate predicate;
    private final Locale locale = Locale.US;
    private Writer writer;

    private final Map<Class<?>, MetricRenderer<? extends Metric>> renderers = new HashMap<Class<?>, MetricRenderer<? extends Metric>>();
    private final SocketProvider socketProvider;

    public boolean printVMMetrics = true;

    /**
     * Enables the graphite reporter to send data for the default metrics registry
     * to graphite server with the specified period.
     * 
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     */
    public static void enable(long period, TimeUnit unit, String host, int port)
    {
        enable(Metrics.defaultRegistry(), period, unit, host, port);
    }

    /**
     * Enables the graphite reporter to send data for the given metrics registry
     * to graphite server with the specified period.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port)
    {
        enable(metricsRegistry, period, unit, host, port, null);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the
     * specified period.
     * 
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     * @param prefix
     *            the string which is prepended to all metric names
     */
    public static void enable(long period, TimeUnit unit, String host, int port, String prefix)
    {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the
     * specified period.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     * @param prefix
     *            the string which is prepended to all metric names
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix)
    {
        enable(metricsRegistry, period, unit, host, port, prefix, MetricPredicate.ALL);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the
     * specified period.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     * @param prefix
     *            the string which is prepended to all metric names
     * @param predicate
     *            filters metrics to be reported
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix, MetricPredicate predicate)
    {
        try
        {
            final GraphiteReporter reporter = new GraphiteReporter(metricsRegistry, host, port, prefix, predicate);
            reporter.start(period, unit);
        }
        catch(Exception e)
        {
            LOG.error("Error creating/starting Graphite reporter:", e);
        }
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param host
     *            is graphite server
     * @param port
     *            is port on which graphite server is running
     * @param prefix
     *            is prepended to all names reported to graphite
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(String host, int port, String prefix) throws IOException
    {
        this(Metrics.defaultRegistry(), host, port, prefix);
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param host
     *            is graphite server
     * @param port
     *            is port on which graphite server is running
     * @param prefix
     *            is prepended to all names reported to graphite
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix) throws IOException
    {
        this(metricsRegistry, host, port, prefix, MetricPredicate.ALL);
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param host
     *            is graphite server
     * @param port
     *            is port on which graphite server is running
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix, MetricPredicate predicate)
    {
        this(metricsRegistry, prefix, predicate, new DefaultSocketProvider(host, port));
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     * @param socketProvider
     *            provider that provides a {@link Socket}
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider)
    {
        super(metricsRegistry, "graphite-reporter");
        this.socketProvider = socketProvider;

        if(prefix != null)
        {
            // Pre-append the "." so that we don't need to make anything conditional later.
            this.prefix = prefix + ".";
        }
        else
        {
            this.prefix = "";
        }
        this.predicate = predicate;
        this.renderers.put(CounterMetric.class, new MetricRenderer<CounterMetric>()
        {
            @Override
            public String getData(CounterMetric metric, GraphiteReporterAttributes attributes)
            {
                return String.format(attributes.locale, "%s.%s %d %d\n", attributes.name, "count", metric.count(), attributes.epoch);
            }

        });

        this.renderers.put(HistogramMetric.class, new MetricRenderer<HistogramMetric>()
        {
            @Override
            public String getData(HistogramMetric metric, GraphiteReporterAttributes attributes)
            {
                final double[] percentiles = metric.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);
                final StringBuilder lines = new StringBuilder();
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "min", metric.min(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "max", metric.max(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "mean", metric.mean(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "stddev", metric.stdDev(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "median", percentiles[0], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "75percentile", percentiles[1], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "95percentile", percentiles[2], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "98percentile", percentiles[3], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "99percentile", percentiles[4], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "999percentile", percentiles[5], attributes.epoch));

                return lines.toString();
            }
        });

        this.renderers.put(GaugeMetric.class, new MetricRenderer<GaugeMetric<?>>()
        {
            @Override
            public String getData(GaugeMetric<?> metric, GraphiteReporterAttributes attributes)
            {
                return String.format(attributes.locale, "%s.%s %s %d\n", attributes.name, "value", metric.value(), attributes.epoch);
            }
        });

        this.renderers.put(MeterMetric.class, new MetricRenderer<Metered>()
        {
            @Override
            public String getData(Metered metric, GraphiteReporterAttributes attributes)
            {
                final StringBuilder lines = new StringBuilder();
                lines.append(String.format(attributes.locale, "%s.%s %d %d\n", attributes.name, "count", metric.count(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "meanRate", metric.meanRate(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "1MinuteRate", metric.oneMinuteRate(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "5MinuteRate", metric.fiveMinuteRate(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "15MinuteRate", metric.fifteenMinuteRate(), attributes.epoch));

                return lines.toString();
            }
        });

        this.renderers.put(TimerMetric.class, new MetricRenderer<TimerMetric>()
        {
            @Override
            public String getData(TimerMetric metric, GraphiteReporterAttributes attributes)
            {
                final double[] percentiles = metric.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);

                final StringBuilder lines = new StringBuilder();
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "min", metric.min(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "max", metric.max(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "mean", metric.mean(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "stddev", metric.stdDev(), attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "median", percentiles[0], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "75percentile", percentiles[1], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "95percentile", percentiles[2], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "98percentile", percentiles[3], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "99percentile", percentiles[4], attributes.epoch));
                lines.append(String.format(attributes.locale, "%s.%s %2.2f %d\n", attributes.name, "999percentile", percentiles[5], attributes.epoch));

                return lines.toString();
            }
        });
    }

    /**
     * Register custom Graphite renderers
     * 
     * @param metricType
     *            the type of metric to register a renderer for
     * @param renderer
     *            the renderer to register for the given type
     */
    public <T extends Metric, Y extends T> void registerRenderer(Class<Y> metricType, MetricRenderer<T> renderer)
    {
        this.renderers.put(metricType, renderer);
    }

    @Override
    public void run()
    {
        runInternal(System.currentTimeMillis() / 1000);
    }
    
    void runInternal(long epoch)
    {
        Socket socket = null;
        try
        {
            socket = this.socketProvider.get();
            this.writer = new OutputStreamWriter(socket.getOutputStream());

            
            if(this.printVMMetrics)
            {
                printVmMetrics(epoch);
            }
            printRegularMetrics(epoch);
            this.writer.flush();
        }
        catch(Exception e)
        {
            if(LOG.isDebugEnabled())
            {
                LOG.debug("Error writing to Graphite", e);
            }
            else
            {
                LOG.warn("Error writing to Graphite: {}", e.getMessage());
            }
            if(this.writer != null)
            {
                try
                {
                    this.writer.flush();
                }
                catch(IOException e1)
                {
                    LOG.error("Error while flushing writer:", e1);
                }
            }
        }
        finally
        {
            if(socket != null)
            {
                try
                {
                    socket.close();
                }
                catch(IOException e)
                {
                    LOG.error("Error while closing socket:", e);
                }
            }
            this.writer = null;
        }
    }
    private void printRegularMetrics(long epoch)
    {
        for(Entry<String, Map<String, Metric>> entry : Utils.sortAndFilterMetrics(this.metricsRegistry.allMetrics(), this.predicate).entrySet())
        {
            for(Entry<String, Metric> subEntry : entry.getValue().entrySet())
            {
                final String name = sanitizeName(this.prefix + entry.getKey() + "." + subEntry.getKey());

                final Metric metric = subEntry.getValue();

                if(metric != null)
                {
                    try
                    {
                        metric.renderMetric(this, new GraphiteReporterAttributes(name, epoch, this.locale));

                    }
                    catch(Exception ignored)
                    {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }
    }

    private void sendToGraphite(String data)
    {
        try
        {
            this.writer.write(data);
        }
        catch(IOException e)
        {
            LOG.error("Error sending to Graphite:", e);
        }
    }

    private static String sanitizeName(String name)
    {
        return name.replace(' ', '-');
    }

    private void printDoubleField(String name, double value, long epoch)
    {
        sendToGraphite(String.format(this.locale, "%s%s %2.2f %d\n", this.prefix, sanitizeName(name), value, epoch));
    }

    private void printLongField(String name, long value, long epoch)
    {
        sendToGraphite(String.format(this.locale, "%s%s %d %d\n", this.prefix, sanitizeName(name), value, epoch));
    }

    private void printVmMetrics(long epoch)
    {
        printDoubleField("jvm.memory.heap_usage", heapUsage(), epoch);
        printDoubleField("jvm.memory.non_heap_usage", nonHeapUsage(), epoch);
        for(Entry<String, Double> pool : memoryPoolUsage().entrySet())
        {
            printDoubleField("jvm.memory.memory_pool_usages." + pool.getKey(), pool.getValue(), epoch);
        }

        printDoubleField("jvm.daemon_thread_count", daemonThreadCount(), epoch);
        printDoubleField("jvm.thread_count", threadCount(), epoch);
        printDoubleField("jvm.uptime", uptime(), epoch);
        printDoubleField("jvm.fd_usage", fileDescriptorUsage(), epoch);

        for(Entry<State, Double> entry : threadStatePercentages().entrySet())
        {
            printDoubleField("jvm.thread-states." + entry.getKey().toString().toLowerCase(), entry.getValue(), epoch);
        }

        for(Entry<String, GarbageCollector> entry : garbageCollectors().entrySet())
        {
            printLongField("jvm.gc." + entry.getKey() + ".time", entry.getValue().getTime(TimeUnit.MILLISECONDS), epoch);
            printLongField("jvm.gc." + entry.getKey() + ".runs", entry.getValue().getRuns(), epoch);
        }
    }

    @Override
    public void renderCounter(CounterMetric counterMetric, RenderAttributes attributes)
    {
        GraphiteReporterAttributes graphiteAttributes = (GraphiteReporterAttributes)attributes;

        MetricRenderer<CounterMetric> renderer = (MetricRenderer<CounterMetric>)this.renderers.get(CounterMetric.class);

        sendToGraphite(renderer.getData(counterMetric, graphiteAttributes));
    }

    @Override
    public void renderHistogram(HistogramMetric histogramMetric, RenderAttributes attributes)
    {
        GraphiteReporterAttributes graphiteAttributes = (GraphiteReporterAttributes)attributes;

        MetricRenderer<HistogramMetric> renderer = (MetricRenderer<HistogramMetric>)this.renderers.get(HistogramMetric.class);

        sendToGraphite(renderer.getData(histogramMetric, graphiteAttributes));
    }

    @Override
    public void renderGauge(GaugeMetric<?> gauge, RenderAttributes attributes)
    {
        GraphiteReporterAttributes graphiteAttributes = (GraphiteReporterAttributes)attributes;

        MetricRenderer<GaugeMetric> renderer = (MetricRenderer<GaugeMetric>)this.renderers.get(GaugeMetric.class);

        sendToGraphite(renderer.getData(gauge, graphiteAttributes));
    }

    @Override
    public void renderMeter(Metered meterMetric, RenderAttributes attributes)
    {
        GraphiteReporterAttributes graphiteAttributes = (GraphiteReporterAttributes)attributes;

        MetricRenderer<Metered> renderer = (MetricRenderer<Metered>)this.renderers.get(MeterMetric.class);

        sendToGraphite(renderer.getData(meterMetric, graphiteAttributes));

    }

    @Override
    public void renderTimer(TimerMetric timerMetric, RenderAttributes attributes)
    {
        GraphiteReporterAttributes graphiteAttributes = (GraphiteReporterAttributes)attributes;

        renderMeter(timerMetric, attributes);

        MetricRenderer<TimerMetric> renderer = (MetricRenderer<TimerMetric>)this.renderers.get(TimerMetric.class);

        sendToGraphite(renderer.getData(timerMetric, graphiteAttributes));
    }

    private static class DefaultSocketProvider implements SocketProvider
    {
        private final String host;
        private final int port;

        public DefaultSocketProvider(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        @Override
        public Socket get() throws Exception
        {
            return new Socket(this.host, this.port);
        }

    }
}
