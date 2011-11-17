package com.yammer.metrics.core;

import java.io.IOException;

import com.yammer.metrics.reporting.RenderAttributes;
import com.yammer.metrics.reporting.RenderableReporter;

/**
 * A tag interface to indicate that a class is a metric.
 */
public interface Metric {
    public void renderMetric(RenderableReporter reporter, RenderAttributes attributes) throws IOException;
}
