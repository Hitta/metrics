package com.yammer.metrics.core.tests;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.reporting.RenderAttributes;
import com.yammer.metrics.reporting.RenderableReporter;

public class GaugeMetricTest {
    final GaugeMetric<String> gauge = new GaugeMetric<String>() {
        @Override
        public String value() {
            return "woo";
        }

        @Override
        public void renderMetric(RenderableReporter reporter, RenderAttributes attributes)
        {
        }
    };

    @Test
    public void returnsAValue() throws Exception {
        assertThat("a gauge returns a value",
                   gauge.value(),
                   is("woo"));
    }
}
