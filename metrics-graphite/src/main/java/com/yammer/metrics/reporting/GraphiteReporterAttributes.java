package com.yammer.metrics.reporting;

import java.util.Locale;

public class GraphiteReporterAttributes implements RenderAttributes
{
    final String name;
    final long epoch;
    final Locale locale;

    public GraphiteReporterAttributes(String name, long epoch, Locale locale)
    {
        this.name = name;
        this.epoch = epoch;
        this.locale = locale;
        
    }
}
