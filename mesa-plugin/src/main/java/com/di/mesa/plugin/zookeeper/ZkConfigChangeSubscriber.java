package com.di.mesa.plugin.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * 订阅者实现类，当订阅到zk数据改变时，会触发ConfigChangeListener
 *
 * @author davi
 */
public class ZkConfigChangeSubscriber implements IZkConfigChangeSubscriber {

    private ZkClient zkClient;
    private String rootNode;

    public ZkConfigChangeSubscriber(ZkClient zkClient, String rootNode) {
        this.rootNode = rootNode;
        this.zkClient = zkClient;
    }

    public void subscribe(String key, IZkConfigChangeListener listener) {
        String path = ZkManager.getZkPath(this.rootNode, key);
        if (!this.zkClient.exists(path)) {
            throw new RuntimeException("配置(" + path + ")不存在, 必须先定义配置才能监听配置的变化, 请检查配置的key是否正确, 如果确认配置key正确, 那么需要保证先使用配置发布命令发布配置! ");
        }

        this.zkClient.subscribeDataChanges(path, new DataListenerAdapter(listener));
    }

    /**
     * 触发ConfigChangeListener
     *
     * @param path
     * @param value
     * @param configListener
     */
    private void fireConfigChanged(String path, String value, IZkConfigChangeListener configListener) {
        configListener.configChanged(getKey(path), value);
    }

    private String getKey(String path) {
        String key = path;

        if (!StringUtils.isEmpty(this.rootNode)) {
            key = path.replaceFirst(this.rootNode, "");
            if (key.startsWith("/")) {
                key = key.substring(1);
            }
        }

        return key;
    }

    public String getInitValue(String key) {
        String path = ZkManager.getZkPath(this.rootNode, key);
        return (String) this.zkClient.readData(path);
    }

    public List<String> listKeys() {
        if (!this.zkClient.exists(this.rootNode)) {
            ZkManager.mkPaths(this.zkClient, this.rootNode);
        }

        return this.zkClient.getChildren(this.rootNode);
    }

    /**
     * 数据监听器适配类，当zk数据变化时，触发ConfigChangeListener
     *
     * @author davi
     */
    private class DataListenerAdapter implements IZkDataListener {
        private IZkConfigChangeListener configListener;

        public DataListenerAdapter(IZkConfigChangeListener configListener) {
            this.configListener = configListener;
        }

        public void handleDataChange(String s, Object obj) throws Exception {
            ZkConfigChangeSubscriber.this.fireConfigChanged(s, (String) obj, this.configListener);
        }

        public void handleDataDeleted(String s) throws Exception {
        }
    }
}