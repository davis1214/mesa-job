package com.di.mesa.metric.servlet;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.di.mesa.metric.AlarmServer;
import com.di.mesa.metric.common.ServerContants;
import com.di.mesa.metric.model.ExecutorInfo;
import com.di.mesa.metric.model.MAlarm;
import com.di.mesa.metric.util.PropertyUtil;
import com.di.mesa.metric.util.JSONUtils;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerStatisticsServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final int cacheTimeInMilliseconds = 1000;
    private static final Logger logger = LoggerFactory.getLogger(ServerStatisticsServlet.class);
    private static final String noCacheParamName = "nocache";
    private static final boolean exists_Bash = new File("/bin/bash").exists();
    private static final boolean exists_Cat = new File("/bin/cat").exists();
    private static final boolean exists_Grep = new File("/bin/grep").exists();
    // private static final boolean exists_Grep = new
    // File("/usr/bin/grep").exists();
    private static final boolean exists_Meminfo = new File("/proc/meminfo").exists();
    private static final boolean exists_LoadAvg = new File("/proc/loadavg").exists();

    protected static long lastRefreshedTime = 0;
    protected static ExecutorInfo cachedstats = null;
    private AlarmServer alarmServer;

    @Override
    public void init(ServletConfig config) throws ServletException {
        logger.info("executor servlet init");

        alarmServer = (AlarmServer) config.getServletContext().getAttribute(ServerContants.DI_AlARM_SERVLET_CONTEXT_KEY);
        this.populateStatistics();
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        boolean noCache = null != req && Boolean.valueOf(req.getParameter(noCacheParamName));

        if (noCache || System.currentTimeMillis() - lastRefreshedTime > cacheTimeInMilliseconds) {
            this.populateStatistics();
        }

        JSONUtils.toJSON(cachedstats, resp.getOutputStream(), true);
    }

    protected void fillRemainingMemoryPercent(ExecutorInfo stats) {
        if (exists_Bash && exists_Cat && exists_Grep && exists_Meminfo) {
            java.lang.ProcessBuilder processBuilder = new java.lang.ProcessBuilder("/bin/bash", "-c",
                    "/bin/cat /proc/meminfo | grep -E '^MemTotal:|^MemFree:|^Buffers:|^Cached:|^SwapCached:'");
            try {
                ArrayList<String> output = new ArrayList<String>();
                Process process = processBuilder.start();
                process.waitFor();
                InputStream inputStream = process.getInputStream();
                try {
                    java.io.BufferedReader reader = new java.io.BufferedReader(new InputStreamReader(inputStream));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        output.add(line);
                    }
                } finally {
                    inputStream.close();
                }

                long totalMemory = 0;
                long totalFreeMemory = 0;
                Long parsedResult = (long) 0;

                // process the output from bash call.
                // we expect the result from the bash call to be something like
                // following -
                // MemTotal: 65894264 kB
                // MemFree: 57753844 kB
                // Buffers: 305552 kB
                // Cached: 3802432 kB
                // SwapCached: 0 kB
                // Note : total free memory = freeMemory + cached + buffers +
                // swapCached
                // TODO : think about merging the logic in systemMemoryInfo as
                // the logic is similar
                if (output.size() == 5) {
                    for (String result : output) {
                        // find the total memory and value the variable.
                        parsedResult = extractMemoryInfo("MemTotal", result);
                        if (null != parsedResult) {
                            totalMemory = parsedResult;
                            continue;
                        }

                        // find the free memory.
                        parsedResult = extractMemoryInfo("MemFree", result);
                        if (null != parsedResult) {
                            totalFreeMemory += parsedResult;
                            continue;
                        }

                        // find the Buffers.
                        parsedResult = extractMemoryInfo("Buffers", result);
                        if (null != parsedResult) {
                            totalFreeMemory += parsedResult;
                            continue;
                        }

                        // find the Cached.
                        parsedResult = extractMemoryInfo("SwapCached", result);
                        if (null != parsedResult) {
                            totalFreeMemory += parsedResult;
                            continue;
                        }

                        // find the Cached.
                        parsedResult = extractMemoryInfo("Cached", result);
                        if (null != parsedResult) {
                            totalFreeMemory += parsedResult;
                            continue;
                        }
                    }
                } else {
                    logger.error("failed to get total/free memory info as the bash call returned invalid result."
                            + String.format(" Output from the bash call - %s ", output.toString()));
                }

                // the number got from the proc file is in KBs we want to see
                // the number in MBs so we are dividing it by 1024.
                stats.setRemainingMemoryInMB(totalFreeMemory / 1024);
                stats.setRemainingMemoryPercent(totalMemory == 0 ? 0
                        : ((double) totalFreeMemory / (double) totalMemory) * 100);
            } catch (Exception ex) {
                logger.error("failed fetch system memory info "
                        + "as exception is captured when fetching result from bash call. Ex -" + ex.getMessage());
            }
        } else {
            logger.error("failed fetch system memory info, one or more files from the following list are missing -  "
                    + "'/bin/bash'," + "'/bin/cat'," + "'/proc/loadavg'");
        }
    }

    private Long extractMemoryInfo(String field, String result) {
        Long returnResult = null;
        if (null != result && null != field && result.matches(String.format("^%s:.*", field))
                && result.split("\\s+").length > 2) {
            try {
                returnResult = Long.parseLong(result.split("\\s+")[1]);
                logger.debug(field + ":" + returnResult);
            } catch (NumberFormatException e) {
                returnResult = 0L;
                logger.error(String.format("yielding 0 for %s as output is invalid - %s", field, result));
            }
        }
        return returnResult;
    }

    protected synchronized void populateStatistics() {
        final ExecutorInfo stats = new ExecutorInfo();
        fillRemainingMemoryPercent(stats);
        fillCpuUsage(stats);
        fillRemainingAlarmCapacityAndLastDispatchedTime(stats);
        cachedstats = stats;
        lastRefreshedTime = System.currentTimeMillis();
    }

    /**
     * fill the result set with the remaining flow capacity .
     *
     * @param stats reference to the result container which contains all the
     *              results, this specific method will only work on the property
     *              "remainingAlarmCapacity".
     */
    protected void fillRemainingAlarmCapacityAndLastDispatchedTime(ExecutorInfo stats) {
        if (alarmServer != null) {
            try {
                List<MAlarm> list = alarmServer.getJobTaskManager().getAllJob();
                stats.setNumberOfAssignedAlarms(PropertyUtil.getIntegerValueByKey("alarm.properties", "alarm.capacity"));
                stats.setRemainingAlarmCapacity(stats.getNumberOfAssignedAlarms() - list.size());
                stats.setLastDispatchedTime(alarmServer.getJobTaskManager().getLastAlarmSubmitTime());
            } catch (SchedulerException e) {
                logger.error(e.getMessage(), e);
            }
        } else {
            logger.error("failed to get data for remaining flow capacity or LastDispatchedTime"
                    + " as the AlarmServer has yet been initialized.");
        }
    }

    /**
     * <pre>
     * fill the result set with the CPU usage .
     * Note : As the 'Top' bash call doesn't yield accurate result for the system load,
     *        the implementation has been changed to load from the "proc/loadavg" which keeps
     *        the moving average of the system load, we are pulling the average for the recent 1 min.
     * </pre>
     *
     * @param stats reference to the result container which contains all the
     *              results, this specific method will only work on the property
     *              "cpuUsage".
     */
    protected void fillCpuUsage(ExecutorInfo stats) {
        if (exists_Bash && exists_Cat && exists_LoadAvg) {
            java.lang.ProcessBuilder processBuilder = new java.lang.ProcessBuilder("/bin/bash", "-c",
                    "/bin/cat /proc/loadavg");
            try {
                ArrayList<String> output = new ArrayList<String>();
                Process process = processBuilder.start();
                process.waitFor();
                InputStream inputStream = process.getInputStream();
                try {
                    java.io.BufferedReader reader = new java.io.BufferedReader(new InputStreamReader(inputStream));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        output.add(line);
                    }
                } finally {
                    inputStream.close();
                }

                // process the output from bash call.
                if (output.size() > 0) {
                    String[] splitedresult = output.get(0).split("\\s+");
                    double cpuUsage = 0.0;

                    try {
                        cpuUsage = Double.parseDouble(splitedresult[0]);
                    } catch (NumberFormatException e) {
                        logger.error("yielding 0.0 for CPU usage as output is invalid -" + output.get(0));
                    }
                    logger.info("System load : " + cpuUsage);
                    stats.setCpuUpsage(cpuUsage);
                }
            } catch (Exception ex) {
                logger.error("failed fetch system load info "
                        + "as exception is captured when fetching result from bash call. Ex -" + ex.getMessage());
            }
        } else {
            logger.error("failed fetch system load info, one or more files from the following list are missing -  "
                    + "'/bin/bash'," + "'/bin/cat'," + "'/proc/loadavg'");
        }
    }
}
