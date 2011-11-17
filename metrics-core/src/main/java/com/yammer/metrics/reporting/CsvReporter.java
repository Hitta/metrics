package com.yammer.metrics.reporting;

import com.yammer.metrics.core.*;
import com.yammer.metrics.util.MetricPredicate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CsvReporter extends AbstractPollingReporter
{

    private final MetricPredicate predicate;
    private final File outputDir;
    private final Map<MetricName, PrintStream> streamMap;
    private long startTime;

    public CsvReporter(File outputDir,
            MetricsRegistry metricsRegistry,
            MetricPredicate predicate) throws Exception
    {
        super(metricsRegistry, "csv-reporter");
        this.outputDir = outputDir;
        this.predicate = predicate;
        this.streamMap = new HashMap<MetricName, PrintStream>();
        this.startTime = 0L;
    }

    public CsvReporter(File outputDir, MetricsRegistry metricsRegistry)
            throws Exception
    {
        this(outputDir, metricsRegistry, MetricPredicate.ALL);
    }

    private PrintStream getPrintStream(MetricName metricName, String fileHeader)
            throws IOException
    {
        PrintStream stream;
        synchronized(this.streamMap)
        {
            stream = this.streamMap.get(metricName);
            if(stream == null)
            {
                final File newFile = new File(this.outputDir, metricName.getName() + ".csv");
                if(newFile.createNewFile())
                {
                    stream = new PrintStream(new FileOutputStream(newFile));
                    this.streamMap.put(metricName, stream);
                    stream.println(fileHeader);
                }
                else
                {
                    throw new IOException("Unable to create " + newFile);
                }
            }
        }
        return stream;
    }

    @Override
    public void run()
    {
        final long time = (System.currentTimeMillis() - this.startTime) / 1000;
        final Set<Entry<MetricName, Metric>> metrics = this.metricsRegistry.allMetrics().entrySet();
        try
        {
            for(Entry<MetricName, Metric> entry : metrics)
            {
                final MetricName metricName = entry.getKey();
                final Metric metric = entry.getValue();
                if(this.predicate.matches(metricName, metric))
                {
                   metric.renderMetric(this, new CsvReporterAttributes(metricName, time));
                }
            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void start(long period, TimeUnit unit)
    {
        this.startTime = System.currentTimeMillis();
        super.start(period, unit);
    }

    @Override
    public void shutdown()
    {
        try
        {
            super.shutdown();
        }
        finally
        {
            for(PrintStream out : this.streamMap.values())
            {
                out.close();
            }
        }
    }

    private void writeToFile(CsvReporterAttributes csvAttributes, final StringBuilder buf, String fileHeader) throws IOException
    {
        final PrintStream out = getPrintStream(csvAttributes.metricName, fileHeader);
        out.println(buf.toString());
        out.flush();
    }
    
    @Override
    public void renderCounter(CounterMetric counterMetric, RenderAttributes attributes) throws IOException
    {
        CsvReporterAttributes csvAttributes = (CsvReporterAttributes)attributes;

        final StringBuilder buf = new StringBuilder();
        buf.append(csvAttributes.time).append(",").append(counterMetric.count());

        writeToFile(csvAttributes, buf, "# time,count");
    }

    @Override
    public void renderHistogram(HistogramMetric histogramMetric, RenderAttributes attributes) throws IOException
    {
        CsvReporterAttributes csvAttributes = (CsvReporterAttributes)attributes;

        final StringBuilder buf = new StringBuilder();
        buf.append(csvAttributes.time).append(",");
        
        final double[] percentiles = histogramMetric.percentiles(0.5, 0.90, 0.95, 0.99);
        buf.append(histogramMetric.min()).append(",");
        buf.append(histogramMetric.max()).append(",");
        buf.append(histogramMetric.mean()).append(",");
        buf.append(percentiles[0]).append(","); // median
        buf.append(histogramMetric.stdDev()).append(",");
        buf.append(percentiles[1]).append(","); // 90%
        buf.append(percentiles[2]).append(","); // 95%
        buf.append(percentiles[3]); // 99 %
        
        writeToFile(csvAttributes, buf, "# time,min,max,mean,median,stddev,90%,95%,99%");
    }

    @Override
    public void renderGauge(GaugeMetric<?> gaugeMetric, RenderAttributes attributes) throws IOException
    {
        CsvReporterAttributes csvAttributes = (CsvReporterAttributes)attributes;
        
        final StringBuilder buf = new StringBuilder();
        buf.append(csvAttributes.time).append(",");

        final Object objVal = gaugeMetric.value();
        buf.append(objVal);

        writeToFile(csvAttributes, buf, "# time,value");
    }

    @Override
    public void renderMeter(Metered meterMetric, RenderAttributes attributes) throws IOException
    {
        CsvReporterAttributes csvAttributes = (CsvReporterAttributes)attributes;
        
        final StringBuilder buf = new StringBuilder();
        buf.append(csvAttributes.time).append(",");

        buf.append(meterMetric.count()).append(",");
        buf.append(meterMetric.oneMinuteRate()).append(",");
        buf.append(meterMetric.meanRate()).append(",");
        buf.append(meterMetric.fiveMinuteRate()).append(",");
        buf.append(meterMetric.fifteenMinuteRate());

        writeToFile(csvAttributes, buf, "# time,count,1 min rate,mean rate,5 min rate,15 min rate");
    }

    @Override
    public void renderTimer(TimerMetric timerMetric, RenderAttributes attributes) throws IOException
    {
        CsvReporterAttributes csvAttributes = (CsvReporterAttributes)attributes;
        
        final StringBuilder buf = new StringBuilder();
        buf.append(csvAttributes.time).append(",");

        final double[] percentiles = timerMetric.percentiles(0.5, 0.90, 0.95, 0.99);
        buf.append(timerMetric.min()).append(",");
        buf.append(timerMetric.max()).append(",");
        buf.append(timerMetric.mean()).append(",");
        buf.append(percentiles[0]).append(","); // median
        buf.append(timerMetric.stdDev()).append(",");
        buf.append(percentiles[1]).append(","); // 90%
        buf.append(percentiles[2]).append(","); // 95%
        buf.append(percentiles[3]); // 99 %

        writeToFile(csvAttributes, buf, "# time,min,max,mean,median,stddev,90%,95%,99%");
    }

    private class CsvReporterAttributes implements RenderAttributes
    {
        final MetricName metricName;
        final long time;

        public CsvReporterAttributes(MetricName metricName, long time)
        {
            this.metricName = metricName;
            this.time = time;
        }
    }
}
