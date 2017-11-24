package com.di.mesa.metric;

import com.di.mesa.metric.common.CommonProperty;
import com.di.mesa.metric.common.ServerContants;
import com.di.mesa.metric.manager.SchedulerManager;
import com.di.mesa.metric.mbean.JmxMBeanManager;
import com.di.mesa.metric.servlet.ExecutorServlet;
import com.di.mesa.metric.servlet.ServerStatisticsServlet;
import com.di.mesa.metric.util.Props;
import com.di.mesa.metric.util.Utils;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

public class AlarmServer implements AlarmServerMBean {
    private static final Logger logger = LoggerFactory.getLogger(AlarmServer.class);

    public static final int DEFAULT_PORT_NUMBER = 8091;
    public static final int DEFAULT_HEADER_BUFFER_SIZE = 4096;

    private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
    private static final int DEFAULT_THREAD_NUMBER = 50;
    private static final int MAX_FORM_CONTENT_SIZE = 10 * 1024 * 1024;

    private AtomicBoolean stopFlag = new AtomicBoolean(true);

    private int portNumber;
    private String host;
    private long executorId;

    private static AlarmServer alarmServer;
    private Props props;
    private Server server;
    private SchedulerManager jobTaskManager;
    private int alarmCapacity;

    public AlarmServer(Props props) throws Exception {
        this.props = props;

        portNumber = props.getInt("server.port", DEFAULT_PORT_NUMBER);
        host = Utils.getHostIp();

        int maxThreads = props.getInt("executor.maxThreads", DEFAULT_THREAD_NUMBER);
        server = new Server(portNumber);
        QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
        server.setThreadPool(httpThreadPool);

        boolean isStatsOn = props.getBoolean("executor.connector.stats", true);
        logger.info("Setting up connector with stats on: " + isStatsOn);

        for (Connector connector : server.getConnectors()) {
            connector.setStatsOn(isStatsOn);
            logger.info(String.format("Jetty connector name: %s, default header buffer size: %d", connector.getName(),
                    connector.getHeaderBufferSize()));
            connector.setHeaderBufferSize(props.getInt("jetty.headerBufferSize", DEFAULT_HEADER_BUFFER_SIZE));
            logger.info(String.format("Jetty connector name: %s, (if) new header buffer size: %d", connector.getName(),
                    connector.getHeaderBufferSize()));
        }

        Context root = new Context(server, "/mesa", Context.SESSIONS);
        root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
        root.addServlet(new ServletHolder(new ExecutorServlet()), "/execute");
        root.addServlet(new ServletHolder(new ServerStatisticsServlet()), "/statics");
        root.setAttribute(ServerContants.DI_AlARM_SERVLET_CONTEXT_KEY, this);


        this.alarmCapacity = props.getInt("alarm.capacity", 80);
        jobTaskManager = new SchedulerManager(props.getString("opentsdb.query.url",
                "http://192.8.97.120:4242/api/query/?summary=true"), props.getLong("check.delay.time", 240),
                getServerAddress(), alarmCapacity);

        try {
            server.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Utils.croak(e.getMessage(), 1);
        }

        jobTaskManager.init(executorId);
        JmxMBeanManager.getInstance().appendMBean(this);

        logger.info("Alarm Executor Server started on port " + portNumber + " with executor id " + this.executorId);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Jetty Alarm Executor...");
        Props props = CommonProperty.loadProps(args);

        if (props == null) {
            logger.error("Alarm Properties not loaded.");
            logger.error("Exiting Alarm Executor Server...");
            return;
        }

        if (props.containsKey(DEFAULT_TIMEZONE_ID)) {
            String timezone = props.getString(DEFAULT_TIMEZONE_ID);
            System.setProperty("user.timezone", timezone);
            TimeZone.setDefault(TimeZone.getTimeZone(timezone));
            DateTimeZone.setDefault(DateTimeZone.forID(timezone));

            logger.info("Setting timezone to " + timezone);
        }

        alarmServer = new AlarmServer(props);
        alarmServer.addShutdownHook(props.getInt("executor.port", DEFAULT_PORT_NUMBER));
    }

    private void addShutdownHook(int port) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(port));
    }

    private class ShutdownThread extends Thread {
        private int port;

        public ShutdownThread(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try {
                logTopMemoryConsumers();
                logoutExecutor();
            } catch (Exception e) {
                logger.info(("Exception when logging top memory consumers"), e);
            }

            logger.info("Shutting down http server...");
            try {
                alarmServer.stopServer();
            } catch (Exception e) {
                logger.error("Error while shutting down http server.", e);
            }
            logger.info("kk thx bye.");
        }

        private void logoutExecutor() throws SocketException {
            logger.info("logoutExecutor ");
        }

        public void logTopMemoryConsumers() throws Exception, IOException {
            if (new File("/bin/bash").exists() && new File("/bin/ps").exists() && new File("/usr/bin/head").exists()) {
                logger.info("logging top memeory consumer");

                java.lang.ProcessBuilder processBuilder = new java.lang.ProcessBuilder("/bin/bash", "-c",
                        "/bin/ps aux --sort -rss | /usr/bin/head");
                Process p = processBuilder.start();
                p.waitFor();

                InputStream is = p.getInputStream();
                java.io.BufferedReader reader = new java.io.BufferedReader(new InputStreamReader(is));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    logger.info(line);
                }
                is.close();
            }
        }
    }

    public void stopServer() throws Exception {
        server.stop();
        server.destroy();
    }

    public long getExecutorId() {
        return executorId;
    }

    public SchedulerManager getJobTaskManager() {
        return jobTaskManager;
    }

    public void setJobTaskManager(SchedulerManager jobTaskManager) {
        this.jobTaskManager = jobTaskManager;
    }

    public String getServerAddress() {
        return host + ":" + portNumber;
    }

    @Override
    public boolean getStopFlag() {
        return stopFlag.get();
    }

    @Override
    public String getServerState() {
        StringBuilder builder = new StringBuilder();
        // for (Integer partition : taskMap.keySet()) {
        // str.append(partition).append(":").append(!taskMap.get(partition).getErrorFlag()).append(",");
        // }
        return builder.toString();
    }

    @Override
    public void shutdown() {
        stopFlag.set(false);
        try {
            logger.info("Stopping Jetty Alarm Executor[" + this.executorId + "] ...");
            jobTaskManager.stopAllJobs();
            alarmServer.stopServer();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
