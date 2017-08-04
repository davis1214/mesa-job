package com.di.mesa.job.jstorm.configure;

import backtype.storm.Config;

/**
 * Created by davi on 17/8/3.
 */
public class MesaConfig extends Config {

    private static final String DEFAULT_OBJECT = "";

    public static MesaConfig create(){
        return new MesaConfig();
    }

    public MesaConfig() {
    }

    public String getString(String key) {
        Object value = get(key);
        if (value == null) {
            return null;
        }

        return value.toString();
    }

    public String getStringOrDefault(String key, String defaultStr) {
        return getOrDefault(key, defaultStr).toString();
    }


    public Integer getInteger(String key) {
        Object value = get(key);
        if (value == null) {
            return null;
        }

        return Integer.valueOf(value.toString());
    }

    public Integer getIntegerOrDefault(String key, Integer defaultInt) {
        return Integer.valueOf(getOrDefault(key, defaultInt).toString());
    }

}
