package com.di.mesa.tool.zookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * zk配置文件发布者类
 *
 * @author davi
 */
public class ZkConfigPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZkConfigPublisher.class);


    private static void publishConfigs(ZkClient client, String rootNode, File confDir) {
        File[] confs = confDir.listFiles();
        int success = 0;
        int failed = 0;
        for (File conf : confs) {
            if (!conf.isFile()) {
                continue;
            }
            String name = conf.getName();
            String path = ZkManager.getZkPath(rootNode, name);
            ZkManager.mkPaths(client, path);
            String content;
            try {
                content = FileUtils.readFileToString(conf, "UTF-8");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                failed++;
                continue;
            }

            if (!client.exists(path)) {
                try {
                    client.createPersistent(path);
                    client.writeData(path, content);
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    failed++;
                    continue;
                }
                logger.info("Publish file" + conf + " to zookeeper " + path);
            } else {
                try {
                    client.writeData(path, content);
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    failed++;
                    continue;
                }
                logger.info("Publish file" + conf + " to zookeeper " + path);
            }
            success++;
        }
        logger.info("Finished publish ! success " + success + "，failed" + failed + "。");
    }

    public static void main(String[] args) {
        String rootnode = "/hj/conf";
        String file = "sample.properties";
        String confDirPath = "/Users/Administrator/Documents/development/git/zookeeper-step/configYard/src/main/resources/conf";

        ZkClient client = new ZkClient("localhost:2181", 50000);
        client.setZkSerializer(new ZkStringSerializer("UTF-8"));

        File confDir = new File(confDirPath);
        if ((!confDir.exists()) || (!confDir.isDirectory())) {
            logger.info("Error: config path " + confDir + " ! ");
            System.exit(1);
        }

        publishConfigs(client, rootnode, confDir);
    }

}