package com.di.mesa.metric.manager;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuartzManger {

    private static Logger logger = LoggerFactory.getLogger(QuartzManger.class);

    SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    Scheduler scheduler = null;

    {
        try {
            scheduler = schedulerFactory.getScheduler();
            scheduler.start();

            logger.info("scheduler started {" + scheduler.getSchedulerInstanceId() + " - "
                    + scheduler.getSchedulerName() + " }");
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

}
