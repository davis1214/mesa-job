package com.di.mesa.plugin.zookeeper;

import java.util.List;

/**
 * 配置改变的订阅者，在每一個zk文件上订阅一個监听器
 *
 * @author davi
 */
public abstract interface IZkConfigChangeSubscriber {

    public abstract String getInitValue(String paramString);

    public abstract void subscribe(String paramString, IZkConfigChangeListener paramConfigChangeListener);

    public abstract List<String> listKeys();
}