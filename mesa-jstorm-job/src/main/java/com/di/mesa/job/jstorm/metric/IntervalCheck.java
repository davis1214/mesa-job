package com.di.mesa.job.jstorm.metric;

import java.io.Serializable;

/**
 * Created by davi on 17/8/3.
 */
public class IntervalCheck implements Serializable {
    private static final long serialVersionUID = 8952971673547362883L;

    long lastCheck = System.currentTimeMillis();

    // default interval is 1 second
    long interval = 30;

    /*
     * if last check time is before interval seconds, 
     * return true, otherwise false
     */
    public boolean check() {
        return checkAndGet() != null;
    }

    public Double checkAndGet() {
        long now = System.currentTimeMillis();

        synchronized (this) {
            if (now >= interval * 1000 + lastCheck) {
                double pastSecond = ((double) (now - lastCheck)) / 1000;
                lastCheck = now;
                return pastSecond;
            }
        }

        return null;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public void adjust(long addTimeMillis) {
        lastCheck += addTimeMillis;
    }
}
