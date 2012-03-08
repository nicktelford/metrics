package com.yammer.metrics.hadoop;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Verifies the MapReduceReporter correctly tracks metrics in Hadoop counters.
 */
public class MapReduceReporterTest {
    
    TaskInputOutputContext context;
    Counters counters;
    MetricsRegistry registry;
    
    @Before
    public void initCounters() {
        context = mock(TaskInputOutputContext.class);
        counters = new Counters();
        registry = new MetricsRegistry(new Clock() {
            private long val = 0;

            @Override
            public long tick() {
                return val += 50000000;
            }
        });
    }

    @Test
    public void reportIntGauge() throws Exception {
        MetricName name = new MetricName("tests", "test", "test");
        Gauge<Integer> input = registry.newGauge(name, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return 700;
            }
        });
        Counters.Counter output = counters.findCounter("tests", "test");
        MapReduceReporter reporter = new MapReduceReporter();

        when(context.getCounter("tests", "test")).thenReturn(output);
        reporter.processGauge(name, input, context);

        assertEquals(700, output.getCounter());
    }
    
    @Test
    public void reportCounter() throws Exception {
        MetricName name = new MetricName("tests", "test", "test");
        Counter input = registry.newCounter(name);
        Counters.Counter output = counters.findCounter("tests", "test_count");
        MapReduceReporter reporter = new MapReduceReporter();
        
        input.inc(300);
        input.inc(400);
        
        when(context.getCounter("tests", "test_count")).thenReturn(output);
        reporter.processCounter(name, input, context);
        
        assertEquals(700, output.getCounter());
    }

    @Test
    public void reportTimer() throws Exception {
        MetricName name = new MetricName("tests", "test", "test");
        Timer input = registry.newTimer(name, TimeUnit.SECONDS, TimeUnit.SECONDS);
        Counters.Counter count = counters.findCounter("tests", "test_count");
        Counters.Counter total = counters.findCounter("tests", "test_total");
        MapReduceReporter reporter = new MapReduceReporter();

        input.update(300, TimeUnit.SECONDS);
        input.update(400, TimeUnit.SECONDS);

        when(context.getCounter("tests", "test_count")).thenReturn(count);
        when(context.getCounter("tests", "test_total")).thenReturn(total);
        reporter.processTimer(name, input, context);

        assertEquals(2, count.getCounter());
        assertEquals(700, total.getCounter());
    }

    @Test
    public void reportHistogram() throws Exception {
        MetricName name = new MetricName("tests", "test", "test");
        Histogram input = registry.newHistogram(name, false);
        Counters.Counter total = counters.findCounter("tests", "test_total");
        MapReduceReporter reporter = new MapReduceReporter();

        input.update(300);
        input.update(400);

        when(context.getCounter("tests", "test_total")).thenReturn(total);
        reporter.processHistogram(name, input, context);

        assertEquals(700, total.getCounter());
    }
    
    @Test
    public void reportAll() throws Exception {
        MetricName name = new MetricName("tests", "test", "test");
        Counter input = Metrics.defaultRegistry().newCounter(name);
        Counters.Counter output = counters.findCounter("tests", "test_count");

        input.inc(300);
        input.inc(400);

        when(context.getCounter("tests", "test_count")).thenReturn(output);
        
        MapReduceReporter.reportAll(context);

        assertEquals(700, output.getCounter());
    }
}
