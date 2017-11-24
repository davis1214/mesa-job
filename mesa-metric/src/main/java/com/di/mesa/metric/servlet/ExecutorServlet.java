package com.di.mesa.metric.servlet;

import com.di.mesa.metric.AlarmServer;
import com.di.mesa.metric.common.ServerContants;
import com.di.mesa.metric.manager.SchedulerManager;
import com.di.mesa.metric.util.GsonUtil;
import com.di.mesa.metric.common.ConnectorParams;
import com.di.mesa.metric.model.MAlarm;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class ExecutorServlet extends HttpServlet implements ConnectorParams {

    private static final Logger logger = LoggerFactory.getLogger(ExecutorServlet.class);
    public static final String JSON_MIME_TYPE = "application/json";

    private AlarmServer app;
    private SchedulerManager jobTaskManager;
    private String serverAddress;

    @Override
    public void init(ServletConfig config) throws ServletException {
        logger.info("executor servlet init");
        app = (AlarmServer) config.getServletContext().getAttribute(ServerContants.DI_AlARM_SERVLET_CONTEXT_KEY);

        if (app == null) {
            throw new IllegalStateException("No batch application is defined in the servlet context!");
        }

        jobTaskManager = app.getJobTaskManager();
        jobTaskManager.setExecutorId(app.getExecutorId());
        this.serverAddress = app.getServerAddress();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        logger.debug("parameters call " + req.getQueryString());

        HashMap<String, Object> respMap = new HashMap<String, Object>();
        logger.info("ExecutorServer called by " + req.getRemoteAddr());
        try {
            if (!hasParam(req, TYPE_PARAM)) {
                logger.error("Parameter type not set");
                respMap.put(ConnectorParams.RESPONSE_RESULT, ConnectorParams.RESPONSE_ERROR);
                respMap.put(ConnectorParams.RESPONSE_DESC, "Parameter action not set");
            } else {
                String type = getParam(req, TYPE_PARAM);
                respMap.put(ConnectorParams.TYPE_PARAM, type);
                if (hasParam(req, JOBNAME_PARAM) && hasParam(req, JOBGROUP_PARAM)) {
                    String jobName = getParam(req, JOBNAME_PARAM);
                    String jobGoup = getParam(req, JOBGROUP_PARAM);
                    logger.debug("type " + type + " jobid " + jobName);

                    //judge of type
                    if (!type.equals(SUSPEND_TYPE) && !type.equals(RESUME_TYPE) && !type.equals(KILL_TYPE)) {
                        throw new Exception(
                                "parameter error.it should be <type>(start|suspend|resume|kill) & <id> & <group> or <type>(query) .  for example \n" +
                                        " http://localhost:8009/mesa/execute?type=${type}&id=${mAlarmId}&group=${alarmMetric}");
                    }


                    //judge of job
                    boolean containsJob = jobTaskManager.containsJob(jobName, jobGoup);
                    if (!containsJob) {
                        throw new Exception(
                                "job " + jobName + " on group " + jobGoup + " not exists!");
                    }

                    if (type.equals(SUSPEND_TYPE)) {
                        handleAlarmSuspendRequest(jobName, jobGoup, respMap);
                    } else if (type.equals(RESUME_TYPE)) {
                        handleAlarmResumeRequest(jobName, jobGoup, respMap);
                    } else if (type.equals(KILL_TYPE)) {
                        handleAlarmKillRequest(jobName, jobGoup, respMap);
                    } else {
                        logger.error("parameter error.it should be <type>(start|suspend|resume|kill) & <id> & <group> or <type>(query) .  for example \n" +
                                " http://localhost:8009/mesa/execute?type=${type}&id=${mAlarmId}&group=${alarmMetric}");
                    }
                } else if (type.equals(QUERY_TYPE)) {
                    handleAlarmQueryRequest(respMap);
                } else {
                    throw new Exception(
                            "parameter error.it should be <type>(start|suspend|resume|kill) & <id> & <group> or <type>(query) .  for example \n" +
                                    " http://localhost:8009/mesa/execute?type=${type}&id=${mAlarmId}&group=${alarmMetric}");
                }
                respMap.put(ConnectorParams.RESPONSE_RESULT, ConnectorParams.RESPONSE_SUCCESS);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            respMap.put(ConnectorParams.RESPONSE_RESULT, ConnectorParams.RESPONSE_ERROR);
            respMap.put(ConnectorParams.RESPONSE_DESC, e.getMessage());

            respMap.put(ConnectorParams.RESPONSE_USAGE, "http://localhost:8009/mesa/execute?type=${type}&id=${mAlarmId}&group=${alarmMetric} , type should be \n");
        }

        writeJSON(resp, respMap);
        resp.setCharacterEncoding("utf8");
        resp.flushBuffer();
    }

    private void handleAlarmQueryRequest(HashMap<String, Object> respMap) throws SchedulerException {
        List<MAlarm> list = jobTaskManager.getAllJob();
        respMap.put("jobs", list);
        respMap.put("number", list.size());
        respMap.put(ConnectorParams.RESPONSE_DESC, "all jobs successfully queried");
    }

    /**
     * Put接口方式 , 重启
     *
     * @param alarmData
     * @param respMap
     * @throws Exception
     */
    private void handleAlarmRestartRequest(String alarmData, String jobGroup, HashMap<String, Object> respMap) throws Exception {
        String description = jobTaskManager.restartJob(alarmData, this.serverAddress);
        respMap.put(ConnectorParams.RESPONSE_DESC, description);
    }

    /**
     * Put接口方式 , 启动
     *
     * @param alarmData
     * @throws Exception
     */
    private void handleAlarmStartRequest(String alarmData, String jobGroup, HashMap<String, Object> respMap) throws Exception {
        String description = jobTaskManager.startJob(alarmData, this.serverAddress);
        respMap.put(ConnectorParams.RESPONSE_DESC, description);
    }

    /**
     * Get接口方式 , 启动
     *
     * @param respMap
     * @throws Exception
     */
    private void handleAlarmKillRequest(String jobName, String jobGroup, HashMap<String, Object> respMap) throws SchedulerException {
        String description = jobTaskManager.deleteJob(jobName, jobGroup);
        respMap.put(ConnectorParams.RESPONSE_DESC, description);
    }

    private void handleAlarmResumeRequest(String jobName, String jobGroup, HashMap<String, Object> respMap) throws SchedulerException {
        String description = jobTaskManager.resumeJob(jobName, jobGroup);
        respMap.put(ConnectorParams.RESPONSE_DESC, description);
    }

    private void handleAlarmSuspendRequest(String jobName, String jobGroup, HashMap<String, Object> respMap) throws SchedulerException {
        String description = jobTaskManager.pauseJob(jobName, jobGroup);
        respMap.put(ConnectorParams.RESPONSE_DESC, description);
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {

        HashMap<String, Object> respMap = new HashMap<String, Object>();

        StringBuffer receivedData = new StringBuffer();
        String line = null;
        try {
            req.setCharacterEncoding("utf8");
            BufferedReader reader = req.getReader();
            while ((line = reader.readLine()) != null) {
                receivedData.append(line);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            respMap.put(ConnectorParams.RESPONSE_RESULT, ConnectorParams.RESPONSE_ERROR);
            respMap.put(ConnectorParams.RESPONSE_DESC, "Errors occur:" + e.getMessage());
        }

        try {
            handleAlarmRestartRequest(receivedData.toString(), null, respMap);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            respMap.put(ConnectorParams.RESPONSE_RESULT, ConnectorParams.RESPONSE_ERROR);
            respMap.put(ConnectorParams.RESPONSE_DESC, "Errors occur:" + e.getMessage());
        }

        respMap.put(ConnectorParams.TYPE_PARAM, "start/restart");
        writeJSON(res, respMap);
    }

    protected void writeJSON(HttpServletResponse resp, Object obj) throws IOException {
        resp.setContentType(JSON_MIME_TYPE);
//        ObjectMapper mapper = new ObjectMapper();
//        OutputStream stream = resp.getOutputStream();
//        mapper.writeValue(stream, obj);

        String json = GsonUtil.toJson(obj);
        resp.getWriter().write(json);
    }

    public boolean hasParam(HttpServletRequest request, String param) {
        return request.getParameter(param) != null;
    }

    public String getParam(HttpServletRequest request, String name) throws ServletException {
        String p = request.getParameter(name);
        if (p == null)
            throw new ServletException("Missing required parameter '" + name + "'.");
        else
            return p;
    }

    public String getParam(HttpServletRequest request, String name, String defaultVal) {
        String p = request.getParameter(name);
        if (p == null) {
            return defaultVal;
        }
        return p;
    }

    public int getIntParam(HttpServletRequest request, String name) throws ServletException {
        String p = getParam(request, name);
        return Integer.parseInt(p);
    }

    public int getIntParam(HttpServletRequest request, String name, int defaultVal) {
        if (hasParam(request, name)) {
            try {
                return getIntParam(request, name);
            } catch (Exception e) {
                return defaultVal;
            }
        }
        return defaultVal;
    }

}
