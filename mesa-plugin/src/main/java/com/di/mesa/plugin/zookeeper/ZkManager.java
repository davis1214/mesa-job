package com.di.mesa.plugin.zookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * Created by davi on 17/8/17.
 */
public class ZkManager {

    private static final Logger logger = Logger.getLogger(ZkManager.class);

    public static String getZkPath(String rootNode, String key) {
        if (!StringUtils.isEmpty(rootNode)) {
            if (key.startsWith("/")) {
                key = key.substring(1);
            }
            if (rootNode.endsWith("/")) {
                return rootNode + key;
            }

            return rootNode + "/" + key;
        }

        return key;
    }

    public static void mkPaths(ZkClient client, String path) {
        String[] subs = path.split("\\/");
        if (subs.length < 2) {
            return;
        }
        String curPath = "";
        for (int i = 1; i < subs.length; i++) {
            curPath = curPath + "/" + subs[i];
            if (!client.exists(curPath)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Trying to create zk node: " + curPath);
                }
                client.createPersistent(curPath);
                if (logger.isDebugEnabled())
                    logger.debug("Zk node created successfully: " + curPath);
            }
        }
    }

    public static Properties loadProperties(String file) {
        InputStream inputStream = ZkManager.class.getResourceAsStream(file);
        if (inputStream == null) {
            throw new RuntimeException("No resource file [" + file + "]found");
        }

        Properties props = new Properties();
        try {
            props.load(new BufferedReader(new InputStreamReader(inputStream, "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return props;
    }


}