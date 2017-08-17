package com.di.mesa.plugin.storm;

import backtype.storm.Config;
import com.di.mesa.plugin.storm.spout.MesaSpoutConfigure;
import com.di.mesa.plugin.zookeeper.ZookeeperConfigure;

/**
 * Created by davi on 17/8/3.
 */
public class MesaConfig extends Config {

    private static final String DEFAULT_OBJECT = "";

    public static MesaConfig create() {
        return new MesaConfig();
    }

    public MesaConfig() {
    }

    public void enableZkNotifer(boolean shouldEnableZkNotifer) {
        this.put(CommonConfiure.MESA_TOPOLOGY_ENABLE_NOTIFYER, shouldEnableZkNotifer);
        this.put(MesaSpoutConfigure.MESA_COMPONENT_ID, ZookeeperConfigure.ZOOKEEPER_COMPONENT_ID_DEFAULT);
        this.put(MesaSpoutConfigure.MESA_STREAMING_ID, ZookeeperConfigure.ZOOKEEPER_STREAMING_ID_DEFAULT);
    }

    public boolean shouldEnableNotifer() {
        return this.getBoolean(CommonConfiure.MESA_TOPOLOGY_ENABLE_NOTIFYER);
    }

    public void setNotiferStreamingId(String zkStreamingId) {
        this.put(MesaSpoutConfigure.MESA_STREAMING_ID, zkStreamingId);
    }

    public void setNotiferComponentId(String zkComponentId) {
        this.put(MesaSpoutConfigure.MESA_COMPONENT_ID, zkComponentId);
    }


    public String getNotiferStreamingId() {
        return this.get(MesaSpoutConfigure.MESA_STREAMING_ID).toString();
    }

    public String getNotiferComponentId() {
        return this.get(MesaSpoutConfigure.MESA_COMPONENT_ID).toString();
    }

    public void setTopologyName(String topologyName) {
        this.put(CommonConfiure.MESA_TOPOLOGY_NAME, topologyName);
    }

    public String getTopologyName() {
        return this.getString(CommonConfiure.MESA_TOPOLOGY_NAME);
    }


    public void setRunningMode(String runningMode) {
        this.put(CommonConfiure.MESA_TOPOLOGY_RUNNING_MODE, runningMode);
    }

    public String getRunningMode() {
        return this.getString(CommonConfiure.MESA_TOPOLOGY_RUNNING_MODE);
    }

    public boolean isClusterMode() {
        return !isLocalMode();
    }

    public boolean isLocalMode() {
        boolean isLocalMode = false;

        if (this.containsKey(CommonConfiure.MESA_TOPOLOGY_RUNNING_MODE)) {
            isLocalMode = this.getString(CommonConfiure.MESA_TOPOLOGY_RUNNING_MODE).equals(CommonConfiure.RUNNING_MODE_LOCAL);
        }

        return isLocalMode;
    }

    public String getString(String key) {
        Object value = get(key);
        if (value == null) {
            return null;
        }

        return value.toString();
    }

    public boolean getBoolean(String key) {
        Object value = get(key);
        if (value == null) {
            return false;
        }

        return Boolean.valueOf(value.toString());
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
