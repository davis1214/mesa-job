package com.di.mesa.metric.simple;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KuduStreamingTaskSimpleMetric {
    private ScheduledExecutorService scheduExec;
    private long delayInSeconds = 500;

    public static void main(String[] args) {
        KuduStreamingTaskSimpleMetric simpleMetric = new KuduStreamingTaskSimpleMetric();

        simpleMetric.prepareConfig(args);

        simpleMetric.startScheduledMetric();
    }

    private void startScheduledMetric() {
        // TODO Auto-generated method stub
        System.out.println(new Date() + "  , startScheduledMetric");

        long initialDelay = 5;
        long period = 1 * 60;
        scheduExec.scheduleAtFixedRate(new SimpleMetricThread("di_mesa_kudu_order", delayInSeconds), initialDelay, period,
                TimeUnit.SECONDS);

        scheduExec.scheduleAtFixedRate(new SimpleMetricThread("di_mesa_kudu_vpoint", delayInSeconds), initialDelay, period,
                TimeUnit.SECONDS);

        System.out.println(new Date() + "  , ScheduledMetric has submitted");

    }

    private void prepareConfig(String[] args) {
        System.out.println(new Date() + "  , prepareConfig");
        if (args.length > 1) {
            delayInSeconds = Long.valueOf(args[0]);
        }
        this.scheduExec = Executors.newScheduledThreadPool(3);
    }

}
