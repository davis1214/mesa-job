package azkaban.execapp.job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import azkaban.utils.DateUtil;
import azkaban.utils.JdbcUtil;

public class ProjectDependencyMain {

    public static void main(String[] args) throws Exception {
        ProjectDependencyMain main = new ProjectDependencyMain();
        Map<String, String> parameters = main.getParameter(args);

        main.run(parameters);
    }

    private void run(Map<String, String> parameters) throws Exception {
        System.out.println("正在执行 project " + parameters.get("project") + " , flow " + parameters.get("flow") + " , job " + parameters.get("job") + " , ");

        long submitTime = getSubmitTime(parameters);
        getRelativeDate(parameters, submitTime);

        int retryCount = Integer.valueOf(getParamValue(parameters, "retry.count", "20"));
        int retryTime = Integer.valueOf(getParamValue(parameters, "retry.time", "60000"));

        int execCount = 0;
        boolean shouldStop = false;

        while (!shouldStop) {
            List<FLowJob> flowJobs = query(parameters);
            execCount++;
            boolean jobResultState = true;
            for (FLowJob flowJob : flowJobs) {
                //System.out.println(flowJob);
                jobResultState &= flowJob.getStatus().equals("50");
            }

            System.out.println("第" + execCount + "次执行 " + jobResultState + " ,param " + parameters);

            if (!jobResultState && retryCount > 0 && execCount < retryCount) {
                Thread.sleep(retryTime);
            } else {
                shouldStop = true;
            }
        }
        System.out.println("over");
    }

    private void getRelativeDate(Map<String, String> parameters, long submitTime) {
        // 0d : 和submittime当天的数据
        // -1d : submittime -1 天
        // 0h : 小时
        String relativeDate = getParam(parameters, "relative.date");
        String dateToken = relativeDate.substring(relativeDate.length() - 1);
        String dateTime = relativeDate.substring(0, relativeDate.length() - 1);

        if ("d".equals(dateToken)) {
            submitTime += (Integer.valueOf(dateTime) * 60 * 60 * 1000);
            parameters.put("starttime", DateUtil.getTimesmorning(submitTime) + "");
            parameters.put("endtime", DateUtil.getTimesNight(submitTime) + "");
        } else if ("h".equals(dateToken)) {
            submitTime += (Integer.valueOf(dateTime) * 60 * 1000);
            parameters.put("starttime", DateUtil.getTimesHourBegin(submitTime) + "");
            parameters.put("endtime", DateUtil.getTimesHourEnd(submitTime) + "");
        }
    }

    private long getSubmitTime(Map<String, String> parameters) {

        long submitTime = System.currentTimeMillis();
        try {
            Connection con = JdbcUtil.getInstance(parameters.get("jdbc.url")).getConnection();
            String sql = "SELECT submit_time FROM execution_flows WHERE exec_id = ?";
            PreparedStatement stat = con.prepareStatement(sql);
            stat.setString(1, getParam(parameters, "exec.id"));
            ResultSet result = stat.executeQuery();
            while (result.next()) {
                submitTime = result.getLong("submit_time");
            }
            stat.close();
            JdbcUtil.getInstance(parameters.get("jdbc.url")).release(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.err.println("SQLException: " + ex.getMessage());
        }
        return submitTime;
    }

    private List<FLowJob> query(Map<String, String> parameters) {
        List<FLowJob> flowJobs = new ArrayList<FLowJob>();
        try {
            Connection con = JdbcUtil.getInstance(parameters.get("jdbc.url")).getConnection();

            StringBuffer buffer = new StringBuffer();
            buffer.append(" SELECT p.name,e.flow_id,j.job_id,");
            buffer.append(hasParam(parameters, "job") ? "j.status" : "e.status");
            buffer.append(" FROM execution_flows e, execution_jobs j , projects p  ");
            buffer.append(" WHERE e.project_id = p.id AND e.exec_id = j.exec_id ");
            buffer.append(" AND p.name = ? ");
            buffer.append(" AND e.submit_time between ? and ? ");
            buffer.append(hasParam(parameters, "flow") ? " AND e.flow_id  = ? " : "");
            buffer.append(hasParam(parameters, "job") ? " AND j.job_id = ? " : "");

            System.out.println("sql :" + buffer.toString());
            PreparedStatement stat = con.prepareStatement(buffer.toString());
            stat.setString(1, getParam(parameters, "project"));
            stat.setLong(2, Long.valueOf(getParam(parameters, "starttime")));
            stat.setLong(3, Long.valueOf(getParam(parameters, "endtime")));

            if (hasParam(parameters, "flow")) {
                stat.setString(4, getParam(parameters, "flow"));

                if (hasParam(parameters, "job")) {
                    stat.setString(5, getParam(parameters, "job"));
                }
            } else if (hasParam(parameters, "job")) {
                stat.setString(4, getParam(parameters, "job"));
            }

            ResultSet result = stat.executeQuery();
            while (result.next()) {
                String project = result.getString("name");
                String flow = result.getString("flow_id");
                String job = result.getString("job_id");
                String status = result.getString("status");
                FLowJob flowJob = new FLowJob(project, flow, job, status);
                flowJobs.add(flowJob);
            }
            stat.close();
            JdbcUtil.getInstance(parameters.get("jdbc.url")).release(con);
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.err.println("SQLException: " + ex.getMessage());
        }

        return flowJobs;
    }


    private boolean hasParam(Map<String, String> parameters, String key) {
        if (parameters.get(key) != null && parameters.get(key).length() > 0)
            return true;
        return false;
    }

    private String getParam(Map<String, String> parameters, String key) {
        return parameters.get(key);
    }

	private String getParamValue(Map<String, String> parameters, String key, String defaultValue) {
    	String value = parameters.get(key);
    	if(value!=null&& value.length()>0){
    		return value;
    	}
    	return defaultValue;
    }

    private Map<String, String> getParameter(String[] args) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        final Options options = createOptions();
        final CommandLineParser parser = new BasicParser();
        final CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption("h")) {
            showHelp(options);
        }

        final String retryCount = getValue(commandLine, "c", "10");
        final String retryTime = getValue(commandLine, "t", "60s");
        final String relativeDate = getValue(commandLine, "d", "0d");
        final String project = getValue(commandLine, "p", "");
        final String flow = getValue(commandLine, "f", "");
        final String job = getValue(commandLine, "j", "");
        final String jdbcUrl = getValue(commandLine, "jc", "");

        if (project.isEmpty() || jdbcUrl.isEmpty()) {
            System.out.println("You must specify project,flow,job!");
            showHelp(options);
            throw new Exception("You must specify project,flow,job!");
        }

        parameters.put("retry.count", retryCount);
        parameters.put("retry.time", retryTime);
        parameters.put("relative.date", relativeDate);
        parameters.put("project", project);
        parameters.put("flow", flow);
        parameters.put("job", job);
        parameters.put("jdbc.url", jdbcUrl);

        List<String> otherParam = new ArrayList<String>();
        for (int i = 2 * parameters.size(); i < args.length; i++) {
            otherParam.add(args[i]);
        }

        if (otherParam.size() > 0)
            parameters.put("run.day.time", otherParam.get(0));

        parameters.put("exec.id", System.getProperty("azkaban.execid"));

        return parameters;
    }

    private String getValue(CommandLine commandLine, String option, String defaultValue) {
        String optionValue = commandLine.getOptionValue(option);
        if (optionValue == null)
            return defaultValue;
        return optionValue.trim();
    }

    private static Options createOptions() {
        final Options options = new Options();
        options.addOption("c", "retry.count", true, "retry.count");
        options.addOption("t", "retry.time", true, "retry time");
        options.addOption("d", "relative.date", true, "relative.date");
        options.addOption("jc", "jdbc.url", true, "jdbc.url");
        options.addOption("p", "project", true, "project");
        options.addOption("f", "flow", true, "flow");
        options.addOption("j", "job", true, "job");
        options.addOption("h", "help", false, "Help");
        return options;
    }

    private static void showHelp(final Options options) {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("help", options);
        System.exit(-1);
    }

    private class FLowJob {
        private String project;
        private String flow;
        private String job;
        private String status;

        public FLowJob(String project, String flow, String job, String status) {
            this.project = project;
            this.flow = flow;
            this.job = job;
            this.status = status;
        }

        @Override
        public String toString() {
            return "FLowJob{" +
                    "project='" + project + '\'' +
                    ", flow='" + flow + '\'' +
                    ", job='" + job + '\'' +
                    ", status='" + status + '\'' +
                    '}';
        }

        public String getProject() {
            return project;
        }

        public void setProject(String project) {
            this.project = project;
        }

        public String getFlow() {
            return flow;
        }

        public void setFlow(String flow) {
            this.flow = flow;
        }

        public String getJob() {
            return job;
        }

        public void setJob(String job) {
            this.job = job;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

}
