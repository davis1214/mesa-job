package com.di.mesa.tool.zookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.lang.System.out;

/**
 * @author davi
 */
public class ZkConfigReader {

    private static final Logger logger = LoggerFactory.getLogger(ZkConfigReader.class);

    /**
     * read  configurations from configurations
     *
     * @param client
     * @param rootNode
     * @param configFileName
     * @return
     */
    private static String readConfigFile(ZkClient client, String rootNode, String configFileName) {
        String configFile = rootNode + "/" + configFileName;
        String content = null;

        boolean isExists = client.exists(configFile);
        if (isExists) {
            content = (String) client.readData(configFile);
            logger.info("read config file {} ,content {} ", configFile, content);
        }

        return content;
    }

    /**
     * download configurations from root node to local path files
     *
     * @param client
     * @param rootNode
     * @param confDir
     */
    public static void downloadConfigFiles(ZkClient client, String rootNode, String confDir) {
        List<String> configs = client.getChildren(rootNode);

        for (String config : configs) {
            String content = (String) client.readData(rootNode + "/" + config);
            File confFile = new File(confDir, config);
            try {
                FileUtils.writeStringToFile(confFile, content, "UTF-8");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("save configuration [{}] to ", content, confFile.getAbsolutePath());
        }
    }

    /**
     * download all configurations from root node to local path
     *
     * @param client
     * @param rootNode
     * @param targetConfigFile
     * @param parentConfDir
     */
    public static void downloadConfigFile(ZkClient client, String rootNode, String targetConfigFile, String parentConfDir) {
        String configFile = rootNode + "/" + targetConfigFile;
        boolean isExists = client.exists(targetConfigFile);

        if (isExists) {
            String content = (String) client.readData(configFile);
            File confFile = new File(parentConfDir, parentConfDir);
            try {
                FileUtils.writeStringToFile(confFile, content, "UTF-8");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }

            logger.info("save configuration [{}] to ", content, confFile.getAbsolutePath());
        }
    }

    public static void main(String[] args) {

        String rootnode = "/hj/conf";
        String file = "sample.properties";

        ZkClient client = new ZkClient("localhost:2181", 50000);
        client.setZkSerializer(new ZkStringSerializer("UTF-8"));

        String content = readConfigFile(client, rootnode, file);

        out.println("content -> " + content);
    }


}