package com.di.mesa.plugin.zookeeper;


public abstract interface IZkConfigChangeListener {
    public abstract void configChanged(String paramString1, String paramString2);
}