package com.yammer.metrics.reporting;

import com.yammer.metrics.core.Metric;

public interface MetricRenderer<T extends Metric>
{
    String getData(T metric, GraphiteReporterAttributes attributes);
}
