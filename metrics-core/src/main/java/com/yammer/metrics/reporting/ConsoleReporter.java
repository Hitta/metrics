package com.yammer.metrics.reporting;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.util.MetricPredicate;
import com.yammer.metrics.util.Utils;

/**
 * A simple reporters which prints out application metrics to a
 * {@link PrintStream} periodically.
 */
public class ConsoleReporter extends AbstractPollingReporter {
    private final PrintStream out;
    private final MetricPredicate predicate;

    /**
     * Enables the console reporter for the default metrics registry, and causes it to
     * print to STDOUT with the specified period.
     *
     * @param period the period between successive outputs
     * @param unit   the time unit of {@code period}
     */
    public static void enable(long period, TimeUnit unit) {
        enable(Metrics.defaultRegistry(), period, unit);
    }

    /**
     * Enables the console reporter for the given metrics registry, and causes
     * it to print to STDOUT with the specified period and unrestricted output.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit) {
        final ConsoleReporter reporter = new ConsoleReporter(metricsRegistry, System.out, MetricPredicate.ALL);
        reporter.start(period, unit);
    }

    /**
     * Creates a new {@link ConsoleReporter} for the default metrics registry, with unrestricted output.
     *
     * @param out the {@link java.io.PrintStream} to which output will be written
     */
    public ConsoleReporter(PrintStream out) {
        this(Metrics.defaultRegistry(), out, MetricPredicate.ALL);
    }

    /**
     * Creates a new {@link ConsoleReporter} for a given metrics registry.
     *
     * @param metricsRegistry the metrics registry
     * @param out             the {@link java.io.PrintStream} to which output will be written
     * @param predicate       the {@link MetricPredicate} used to determine whether a metric will be output
     */
    public ConsoleReporter(MetricsRegistry metricsRegistry, PrintStream out, MetricPredicate predicate) {
        super(metricsRegistry, "console-reporter");
        this.out = out;
        this.predicate = predicate;
    }

    @Override
    public void run() {
        try {
            final DateFormat format = SimpleDateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);
            final String dateTime = format.format(new Date());
            out.print(dateTime);
            out.print(' ');
            for (int i = 0; i < (80 - dateTime.length() - 1); i++) {
                out.print('=');
            }
            out.println();

            for (Entry<String, Map<String, Metric>> entry : Utils.sortAndFilterMetrics(metricsRegistry.allMetrics(), predicate).entrySet()) {
                out.print(entry.getKey());
                out.println(':');

                for (Entry<String, Metric> subEntry : entry.getValue().entrySet()) {
                    out.print("  ");
                    out.print(subEntry.getKey());
                    out.println(':');
                    
                    final Metric metric = subEntry.getValue();
                    
                    metric.renderMetric(this, null);
                    
                    out.println();
                }
                out.println();
            }
            out.println();
            out.flush();
        } catch (Exception e) {
            e.printStackTrace(out);
        }
    }

    private static String abbrev(TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                return "ns";
            case MICROSECONDS:
                return "us";
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
        }
        throw new IllegalArgumentException("Unrecognized TimeUnit: " + unit);
    }

    @Override
    public void renderCounter(CounterMetric counterMetric, RenderAttributes attributes)
    {
        out.print("    count = ");
        out.println(counterMetric.count());
    }

    @Override
    public void renderHistogram(HistogramMetric histogramMetric, RenderAttributes attributes)
    {
        final double[] percentiles = histogramMetric.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);
        out.printf("               min = %2.2f\n", histogramMetric.min());
        out.printf("               max = %2.2f\n", histogramMetric.max());
        out.printf("              mean = %2.2f\n", histogramMetric.mean());
        out.printf("            stddev = %2.2f\n", histogramMetric.stdDev());
        out.printf("            median = %2.2f\n", percentiles[0]);
        out.printf("              75%% <= %2.2f\n", percentiles[1]);
        out.printf("              95%% <= %2.2f\n", percentiles[2]);
        out.printf("              98%% <= %2.2f\n", percentiles[3]);
        out.printf("              99%% <= %2.2f\n", percentiles[4]);
        out.printf("            99.9%% <= %2.2f\n", percentiles[5]);
    }

    @Override
    public void renderGauge(GaugeMetric<?> gauge, RenderAttributes attributes)
    {
        out.print("    value = ");
        out.println(gauge.value());
    }

    @Override
    public void renderMeter(Metered meterMetric, RenderAttributes attributes)
    {
        final String unit = abbrev(meterMetric.rateUnit());
        out.printf("             count = %d\n", meterMetric.count());
        out.printf("         mean rate = %2.2f %s/%s\n", meterMetric.meanRate(), meterMetric.eventType(), unit);
        out.printf("     1-minute rate = %2.2f %s/%s\n", meterMetric.oneMinuteRate(), meterMetric.eventType(), unit);
        out.printf("     5-minute rate = %2.2f %s/%s\n", meterMetric.fiveMinuteRate(), meterMetric.eventType(), unit);
        out.printf("    15-minute rate = %2.2f %s/%s\n", meterMetric.fifteenMinuteRate(), meterMetric.eventType(), unit);
    }

    @Override
    public void renderTimer(TimerMetric timerMetric, RenderAttributes attributes)
    {
        renderMeter(timerMetric, attributes);

        final String durationUnit = abbrev(timerMetric.durationUnit());

        final double[] percentiles = timerMetric.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);
        out.printf("               min = %2.2f%s\n", timerMetric.min(), durationUnit);
        out.printf("               max = %2.2f%s\n", timerMetric.max(), durationUnit);
        out.printf("              mean = %2.2f%s\n", timerMetric.mean(), durationUnit);
        out.printf("            stddev = %2.2f%s\n", timerMetric.stdDev(), durationUnit);
        out.printf("            median = %2.2f%s\n", percentiles[0], durationUnit);
        out.printf("              75%% <= %2.2f%s\n", percentiles[1], durationUnit);
        out.printf("              95%% <= %2.2f%s\n", percentiles[2], durationUnit);
        out.printf("              98%% <= %2.2f%s\n", percentiles[3], durationUnit);
        out.printf("              99%% <= %2.2f%s\n", percentiles[4], durationUnit);
        out.printf("            99.9%% <= %2.2f%s\n", percentiles[5], durationUnit);
    }
}
