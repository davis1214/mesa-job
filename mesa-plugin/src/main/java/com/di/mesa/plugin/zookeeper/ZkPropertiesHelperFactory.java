package com.di.mesa.plugin.zookeeper;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 动态配置文件辅助类的工厂类，在创建动态配置文件辅助类时，会订阅zk数据改变的事件
 *
 * @author davi
 */
public class ZkPropertiesHelperFactory {

    private IZkConfigChangeSubscriber configChangeSubscriber;
    private ConcurrentHashMap<String, ZkPropertiesHelper> helpers = new ConcurrentHashMap<String, ZkPropertiesHelper>();

    public ZkPropertiesHelperFactory(IZkConfigChangeSubscriber configChangeSubscriber) {
        this.configChangeSubscriber = configChangeSubscriber;
    }

    public ZkPropertiesHelper getHelper(String key) {
        ZkPropertiesHelper helper = (ZkPropertiesHelper) this.helpers.get(key);
        if (helper != null) {
            return helper;
        }

        return createHelper(key);
    }

    /**
     * @param key zk中的一个节点
     * @return
     */
    private ZkPropertiesHelper createHelper(String key) {
        List<String> keys = this.configChangeSubscriber.listKeys();
        if ((keys == null) || (keys.size() == 0)) {
            return null;
        }

        if (!keys.contains(key)) {
            return null;
        }

        String initValue = this.configChangeSubscriber.getInitValue(key);
        final ZkPropertiesHelper helper = new ZkPropertiesHelper(initValue);

        ZkPropertiesHelper old = (ZkPropertiesHelper) this.helpers.putIfAbsent(key, helper);
        if (old != null) {
            return old;
        }

        /**
         * 订阅zk数据改变
         */
        this.configChangeSubscriber.subscribe(key, new IZkConfigChangeListener() {
            public void configChanged(String key, String value) {
                helper.refresh(value);
            }
        });

        return helper;
    }

}