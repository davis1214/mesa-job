/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.di.mesa.metric.common;

import com.di.mesa.metric.util.Props;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;


public abstract class CommonProperty {
    private static final Logger logger = LoggerFactory.getLogger(CommonProperty.class);
    public static final String ALARM_PROPERTIES_FILE = "alarm.properties";
    public static final String ALARM_PRIVATE_PROPERTIES_FILE = "com.di.mesa.metric.alarm.private.properties";
    public static final String DEFAULT_CONF_PATH = "conf";
    private static Props alarmProperties = null;

    public static Props loadProps(String[] args) {
        alarmProperties = loadProps(args, new OptionParser());
        return alarmProperties;
    }

    public static Props getAlarmProperties() {
        return alarmProperties;
    }

    public static Props loadProps(String[] args, OptionParser parser) {
        OptionSpec<String> configDirectory = parser.acceptsAll(Arrays.asList("c", "conf"), "The conf directory for ALARM.").withRequiredArg()
                .describedAs("conf").ofType(String.class);

        Props alarmSettings = null;
        OptionSet options = parser.parse(args);

        if (options.has(configDirectory)) {
            String path = options.valueOf(configDirectory);
            logger.info("Loading ALARM settings file from " + path);
            File dir = new File(path);
            if (!dir.exists()) {
                logger.error("Conf directory " + path + " doesn't exist.");
            } else if (!dir.isDirectory()) {
                logger.error("Conf directory " + path + " isn't a directory.");
            } else {
                alarmSettings = loadConfigurationFromDirectory(dir);
            }
        } else {
            logger.info("Conf parameter not set, attempting to get value from APP_HOME env.");
            alarmSettings = loadConfigurationFromHome();
        }

        return alarmSettings;
    }

    private static Props loadConfigurationFromDirectory(File dir) {
        File alarmPrivatePropsFile = new File(dir, ALARM_PRIVATE_PROPERTIES_FILE);
        File alarmPropsFile = new File(dir, ALARM_PROPERTIES_FILE);

        Props props = null;
        try {
            if (alarmPrivatePropsFile.exists() && alarmPrivatePropsFile.isFile()) {
                logger.info("Loading com.di.mesa.metric.alarm private properties file");
                props = new Props(null, alarmPrivatePropsFile);
            }

            if (alarmPropsFile.exists() && alarmPropsFile.isFile()) {
                logger.info("Loading com.di.mesa.metric.alarm properties file");
                props = new Props(props, alarmPropsFile);
            }
        } catch (FileNotFoundException e) {
            logger.error("File not found. Could not load com.di.mesa.metric.alarm config file", e);
        } catch (IOException e) {
            logger.error("File found, but error reading. Could not load com.di.mesa.metric.alarm config file", e);
        }

        return props;
    }

    /**
     * Loads the ALARM property file from the ALARM_HOME conf directory
     *
     * @return
     */
    private static Props loadConfigurationFromHome() {
        String alarmHome = System.getenv("ALARM_HOME");

        if (alarmHome == null) {
            logger.error("ALARM_HOME not set. Will try default.");
            return null;
        }

        if (!new File(alarmHome).isDirectory() || !new File(alarmHome).canRead()) {
            logger.error(alarmHome + " is not a readable directory.");
            return null;
        }

        File confPath = new File(alarmHome, DEFAULT_CONF_PATH);
        if (!confPath.exists() || !confPath.isDirectory() || !confPath.canRead()) {
            logger.error(alarmHome + " does not contain a readable conf directory.");
            return null;
        }

        return loadConfigurationFromDirectory(confPath);
    }

    public abstract Props getServerProps();


}
