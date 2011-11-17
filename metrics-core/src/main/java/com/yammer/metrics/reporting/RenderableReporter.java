package com.yammer.metrics.reporting;

import java.io.IOException;

import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.TimerMetric;

public interface RenderableReporter
{
    public void renderCounter(CounterMetric counterMetric, RenderAttributes attributes) throws IOException;

    public void renderHistogram(HistogramMetric histogramMetric, RenderAttributes attributes) throws IOException;

    public void renderGauge(GaugeMetric<?> gaugeMetric, RenderAttributes attributes) throws IOException;

    public void renderMeter(Metered meterMetric, RenderAttributes attributes) throws IOException;

    public void renderTimer(TimerMetric timerMetric, RenderAttributes attributes) throws IOException;
}
