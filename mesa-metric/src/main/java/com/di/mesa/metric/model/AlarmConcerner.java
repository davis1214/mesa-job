package com.di.mesa.metric.model;

import com.clearspring.analytics.util.Lists;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlarmConcerner {

    private List<String> tellers;
    private List<String> weixinTeller;
    private List<String> mailTeller;

    public AlarmConcerner(Long alarmId, List<String> tellers, List<String> weixinTeller) {
        super();
        this.tellers = tellers;
        this.weixinTeller = weixinTeller;
    }

    public AlarmConcerner() {

    }

    public boolean needAlarm() {
        if (tellers.size() > 0 || weixinTeller.size() > 0 || mailTeller.size() > 0) {
            return true;
        }

        return false;
    }

    public List<String> getMailTeller() {
        return mailTeller;
    }

    public void setMailTeller(List<String> mailTeller) {
        this.mailTeller = mailTeller;
    }

    public List<String> getTellers() {
        if (tellers == null) {
            tellers = Lists.newArrayList();
        }
        return tellers;
    }

    public void setTellers(List<String> tellers) {
        this.tellers = tellers;
    }

    public List<String> getWeixinTeller() {
        if (weixinTeller == null) {
            weixinTeller = Lists.newArrayList();
        }
        return weixinTeller;
    }

    public void setWeixinTeller(List<String> weixinTeller) {
        this.weixinTeller = weixinTeller;
    }

    public String getWeixinStrs() {
        StringBuffer buffer = new StringBuffer();
        for (String w : weixinTeller) {
            buffer.append(w).append("|");
        }
        return buffer.toString().substring(0, buffer.toString().length() - 1);
    }

}
