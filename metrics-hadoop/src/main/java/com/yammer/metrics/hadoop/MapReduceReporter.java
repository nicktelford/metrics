package com.yammer.metrics.hadoop;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Counter;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Reports limited metrics for MapReduce tasks using Hadoop's counters.
 *
 * Only countable metrics are reported (e.g. "count" and "sum").
 *
 * It's recommended to use another Reporter in conjunction to get the big picture;
 * the MapReduceReporter merely provides additional granularity down to the task
 * level.
 */
public class MapReduceReporter implements MetricProcessor<TaskInputOutputContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceReporter.class);

    /**
     * Reports all Metrics in the default MetricsRegistry as Hadoop Counters.
     * @param context The Hadoop Task Context to report to.
     */
    public static void reportAll(TaskInputOutputContext context) {
        new MapReduceReporter().report(context);
    }

    private final MetricsRegistry registry;
    
    public MapReduceReporter() {
        this(Metrics.defaultRegistry());
    }
    
    public MapReduceReporter(MetricsRegistry registry) {
        this.registry = registry;
    }

    /**
     * Reports all Metrics in this Reporters MetricsRegistry as Hadoop Counters.
     * @param context The Hadoop Task Context to report to.
     */
    public void report(TaskInputOutputContext context) {
        for (Map.Entry<MetricName, Metric> metric: this.registry.allMetrics().entrySet()) {
            try {
                metric.getValue().processWith(this, metric.getKey(), context);
            } catch (Exception ex) {
                LOGGER.warn("Error reporting metric {}", metric.getKey(), ex);
            }
        }
    }

    @Override
    public void processCounter(MetricName name, Counter counter, TaskInputOutputContext context) throws Exception {
        counterFor(context, name, "count").increment(counter.count());
    }
    
    @Override
    public void processMeter(MetricName name, Metered meter, TaskInputOutputContext context) throws Exception {
        counterFor(context, name, "count").increment(meter.count());
        // TODO: showhow report rates
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, TaskInputOutputContext context) throws Exception {
        processSummarizable(name, histogram, context);
        processSampling(name, histogram, context);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, TaskInputOutputContext context) throws Exception {
        processMeter(name, timer, context);
        processSummarizable(name, timer, context);
        processSampling(name, timer, context);
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, TaskInputOutputContext context) throws Exception {
        // TODO: is this wise, won't MR aggregate the values for the parent Job?
        // try to evaluate the Gauge as a Long
        if (gauge.value() instanceof Long) {
            counterFor(context, name).setValue((Long) gauge.value());
        } else if (gauge.value() instanceof Integer) {
            counterFor(context, name).setValue(((Integer) gauge.value()).longValue());
        } else if (gauge.value() instanceof Double) {
            counterFor(context, name).setValue(((Double) gauge.value()).longValue());
        } else if (gauge.value() instanceof Float) {
            counterFor(context, name).setValue(((Float) gauge.value()).longValue());
        } else {
            try {
                // clutching at straws here
                counterFor(context, name).setValue(Long.parseLong(gauge.toString()));
            } catch (NumberFormatException ex) {
                LOGGER.warn("Gauge {} cannot be interpreted as a Long", name, ex);
            }
        }
    }
    
    protected void processSummarizable(MetricName name, Summarizable summarizable, TaskInputOutputContext context) {
        org.apache.hadoop.mapreduce.Counter total = counterFor(context, name, "total");
        total.increment((long) summarizable.sum());

        // TODO: somehow report min, max, stddev and mean
    }
    
    protected void processSampling(MetricName name, Sampling sampling, TaskInputOutputContext context) {
        // TODO: somehow report percentiles and median
    }

    private org.apache.hadoop.mapreduce.Counter counterFor(TaskInputOutputContext context, MetricName name) {
        return counterFor(context, name, null);
    }

    private org.apache.hadoop.mapreduce.Counter counterFor(TaskInputOutputContext context, MetricName name, String suffix) {
        String label = name.getName() + (suffix != null ? "_" + suffix : "");
        return context.getCounter(name.getGroup(), label);
    }
}
