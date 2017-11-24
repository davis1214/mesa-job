package com.di.mesa.metric.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.job.QuartzJob;
import com.di.mesa.metric.model.AlarmConcerner;
import com.di.mesa.metric.model.MAlarm;
import com.di.mesa.metric.util.PropertyUtil;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class SchedulerManager {
    private static final Logger log = LoggerFactory.getLogger(SchedulerManager.class);

    private QuartzManger quartzManger;
    private String opentsdbUrl;
    private long executorId;
    private String runningAddress;
    private long alarmDelayTime;
    private int alarmCapacity;
    private long lastAlarmSubmitTime;

    @PostConstruct
    public void init(long executorId) throws Exception {
        log.info("post construct");
        this.executorId = executorId;
        lastAlarmSubmitTime = System.currentTimeMillis();


//        // TODO 服务重启的时候加载所有已经运行的任务
//        List<MExecutionJobs> mexecutionJobs = alarmDao.getExexcutionJobsByExecutorId(this.executorId);
//        for (MExecutionJobs job : mexecutionJobs) {
//            try {
//                startJob(Integer.valueOf(job.getJobId() + ""), this.runningAddress);
//                log.info(job.getJobName() + " - " + job.getJobId() + " started");
//            } catch (Exception e) {
//                log.error(e.getMessage(), e);
//            }
//        }
    }

    public SchedulerManager(String opentsdbUrl, long alarmDelayTime, String runningAddress, int alarmCapacity) {
        this.opentsdbUrl = opentsdbUrl;
        this.quartzManger = new QuartzManger();
        this.runningAddress = runningAddress;
        this.alarmDelayTime = alarmDelayTime;
        this.alarmCapacity = alarmCapacity;
    }

    public long getExecutorId() {
        return executorId;
    }


    public SchedulerManager setExecutorId(long executorId) {
        this.executorId = executorId;
        return this;
    }

    public int getAlarmCapacity() {
        return alarmCapacity;
    }

    public long getLastAlarmSubmitTime() {
        return lastAlarmSubmitTime;
    }


    /**
     * job 是否存在
     *
     * @param jobName
     * @param jobGroup
     * @throws SchedulerException
     */
    public boolean containsJob(String jobName, String jobGroup) throws SchedulerException {
        Scheduler scheduler = quartzManger.getScheduler();
        JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
        return scheduler.checkExists(jobKey);
    }

    /**
     * 暂停一个job
     *
     * @throws SchedulerException
     */
    public String pauseJob(String jobName, String jobGroup) throws SchedulerException {

        Scheduler scheduler = quartzManger.getScheduler();
        JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
        scheduler.pauseJob(jobKey);
        return "job " + jobName + " on jobGroup " + jobGroup + " successfully paused";
    }

    /**
     * 恢复一个job
     *
     * @throws SchedulerException
     */
    public String resumeJob(String jobName, String jobGroup) throws SchedulerException {
        Scheduler scheduler = quartzManger.getScheduler();
        JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
        scheduler.resumeJob(jobKey);
        return "job " + jobName + " on jobGroup " + jobGroup + " successfully resumed";
    }

    /**
     * 删除一个job
     *
     * @throws SchedulerException
     */
    public String deleteJob(String jobName, String jobGroup) throws SchedulerException {
        Scheduler scheduler = quartzManger.getScheduler();
        JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
        scheduler.deleteJob(jobKey);
        log.info("job " + jobName + " on jobGroup " + jobGroup + " successfully deleted on " + executorId);
        return "job " + jobName + " on jobGroup " + jobGroup + " successfully deleted";
    }


    /**
     * 添加任务
     *
     * @param malarm
     * @throws SchedulerException
     */
    public void addJob(MAlarm malarm) throws SchedulerException {
        if (malarm == null) {
            return;
        }

        // TODO 调整下jobkey 不然会多处很多实例 改成根据topic名称来定一个一个新的实例
        malarm.setJobStatus(AlarmConstants.SCHEDULE_JOB_STATUS_RUNNING);
        Scheduler scheduler = quartzManger.getScheduler();

        String jobName = malarm.getmAlarmId();
        String jobGroup = malarm.getAlarmMetric();

        log.info(" add[" + jobName + "@" + jobGroup + "] to scheduler " + scheduler.getSchedulerName());
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroup);
        SimpleTrigger trigger = (SimpleTrigger) scheduler.getTrigger(triggerKey);
        if (null == trigger) {
            JobDetail jobDetail = JobBuilder.newJob(new QuartzJob().getClass()).withIdentity(jobName, jobGroup).build();

            jobDetail.getJobDataMap().put(AlarmConstants.ALARM_APP_INSTANCE, malarm);

            trigger = TriggerBuilder.newTrigger()
                    .withIdentity("SimplerTigger_" + jobName + "@" + jobGroup)
                    .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever((int) malarm.getAlarmFrequency()))
                    .startNow().build();

            scheduler.scheduleJob(jobDetail, trigger);
        } else {
            trigger = trigger.getTriggerBuilder().withIdentity(triggerKey)
                    .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever((int) malarm.getAlarmFrequency()))
                    .build();
            scheduler.rescheduleJob(triggerKey, trigger);
        }
    }

    /**
     * 获取所有计划中的任务列表
     *
     * @return
     * @throws SchedulerException
     */
    public List<MAlarm> getAllJob() throws SchedulerException {
        Scheduler scheduler = quartzManger.getScheduler();
        GroupMatcher<JobKey> matcher = GroupMatcher.anyJobGroup();
        Set<JobKey> jobKeys = scheduler.getJobKeys(matcher);

        List<MAlarm> jobList = new ArrayList<MAlarm>();
        for (JobKey jobKey : jobKeys) {
            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);

            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            MAlarm mAlarm = (MAlarm) jobDetail.getJobDataMap().get(AlarmConstants.ALARM_APP_INSTANCE);
            //System.out.println(mAlarm);
            for (Trigger trigger : triggers) {
                mAlarm.setDescription("trigger key " + trigger.getKey());
                Trigger.TriggerState triggerState = scheduler.getTriggerState(trigger.getKey());
                mAlarm.setJobStatus(triggerState.name());
                if (trigger instanceof CronTrigger) {
                    CronTrigger cronTrigger = (CronTrigger) trigger;
                    String cronExpression = cronTrigger.getCronExpression();
                    mAlarm.setCronExpression(cronExpression);
                }
                jobList.add(mAlarm);
            }
        }
        return jobList;
    }


    public void stopAllJobs() throws SchedulerException {
        Scheduler scheduler = quartzManger.getScheduler();
        scheduler.shutdown(true);
    }

    //put方法启动的方式进行
    public String startJob(String alarmData, String address) throws Exception {
        lastAlarmSubmitTime = System.currentTimeMillis();
        StringBuffer buffer = new StringBuffer();
        //

        MAlarm malarm = getMAlarm(alarmData);

        try {
            submitOrUpdateQuartzJob(address, buffer, malarm);
        } catch (RuntimeException e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
            throw new Exception("build alarm job error, request parameter  \n" + alarmData + " \non " + address + " error");
        }

        return buffer.toString();
    }


    private MAlarm getMAlarm(String alarmJson) {
        try {
            GsonBuilder builder = new GsonBuilder();
            Gson mapper = builder.create();

            return mapper.fromJson(alarmJson, new TypeToken<MAlarm>() {
            }.getType());
        } catch (Exception e) {
            throw new RuntimeException("convert json to MAlarm error, alarmData" + alarmJson);
        }
    }


    public String restartJob(String alarmId, String address) throws Exception {
        return startJob(alarmId, address);
    }

    private void submitOrUpdateQuartzJob(String address, StringBuffer buffer, MAlarm malarm) throws SchedulerException,
            IOException {

        boolean exists = containsJob(malarm.getmAlarmId(), malarm.getTopic());

        //TODO 统一增加配置信息
        malarm.setEnd(System.currentTimeMillis() / 1000);

        if (malarm.getAlarmConcerner() == null || malarm.getAlarmConcerner().getTellers() == null) {
            AlarmConcerner alarmConcerner = new AlarmConcerner();
            String defaultPhoneTeller = PropertyUtil.getValueByKey("alarm.properties", "default.phone.tellers");
            alarmConcerner.setTellers(Arrays.asList(defaultPhoneTeller.split(",")));
            malarm.setAlarmConcerner(alarmConcerner);
        }

        if (exists) {
            deleteJob(malarm.getmAlarmId(), malarm.getTopic());
            addJob(malarm);
            buffer.append("job ").append(malarm.getmAlarmId()).append(" exists, resumed successfully");
        } else {
            addJob(malarm);
            buffer.append("job ").append(malarm.getmAlarmId()).append(" successfully started");
        }
    }


}
