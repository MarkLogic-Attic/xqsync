/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2004-2022 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.ps.xqsync;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.xcc.Version;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class XQSync {

    //Obtain the version from META-INF/MANIFEST.MF Implementation-Version attribute
    public static final String VERSION = XQSync.class.getPackage().getImplementationVersion();
    private static final String VERSION_MESSAGE = "version " + VERSION
        + " on " + System.getProperty("java.version") + " (" + System.getProperty("java.runtime.name") + ")";

    public static void main(String[] args) throws Exception {

        // make sure the environment is healthy
        String encoding = System.getProperty("file.encoding");
        if (!encoding.equals("UTF-8")) {
            throw new IOException("UTF-8 encoding is required: System property file.encoding " + encoding + " is not UTF-8. "
                + "Change your locale, or set -Dfile.encoding=UTF-8");
        }

        // assume that any input files are properties
        Properties props = new Properties();
        for (String arg : args) {
            try (FileInputStream fileInputStream = new FileInputStream(arg)) {
                props.load(fileInputStream);
            }
            System.err.println("loaded properties from " + arg);
        }

        // allow system properties to override
        props.putAll(System.getProperties());
        System.err.println("added system properties");

        SimpleLogger logger = SimpleLogger.getSimpleLogger();
        logger.configureLogger(props);

        logger.info("XQSync starting: " + VERSION_MESSAGE);
        logger.info("XCC version = " + Version.getVersionString());

        Configuration configuration = initConfiguration(logger, props);

        // repeat startup info to log - and for emphasis
        logger.info("XQSync starting: " + VERSION_MESSAGE);
        logger.info("XCC version = " + Version.getVersionString());

        long start = System.currentTimeMillis();

        // we don't need the manager to be a Thread, so run it directly
        XQSyncManager xqm = new XQSyncManager(configuration);
        xqm.run();

        long duration = System.currentTimeMillis() - start;
        long itemsQueued = xqm.getItemsQueued();
        logger.info("completed " + itemsQueued + " in " + duration + " ms (" + (int) (1000 * itemsQueued / duration) + " docs/s)");
    }

    /**
     * @param logger
     * @param properties
     * @return
     * @throws Exception
     */
    public static synchronized Configuration initConfiguration(SimpleLogger logger, Properties properties) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setLogger((null != logger) ? logger : SimpleLogger.getSimpleLogger());
        configuration.setProperties(properties);

        /*
         * Now that we have a base configuration, we can bootstrap into the
         * correct modularized configuration. This should only be called once,
         * in a single-threaded static context.
         */
        try {
            String configClassName = configuration.getConfigurationClassName();
            logger.info("Configuration is " + configClassName);
            Class<? extends Configuration> configurationClass = Class
                    .forName(configClassName, true, getClassLoader())
                    .asSubclass(Configuration.class);
            Constructor<? extends Configuration> configurationConstructor = configurationClass.getConstructor();
            Properties props = configuration.getProperties();
            configuration = configurationConstructor.newInstance();
            // must pass properties to the new instance
            configuration.setProperties(props);
        } catch (Exception e) {
            throw new FatalException(e);
        }

        // now the configuration is final
        configuration.configure();
        return configuration;
    }

    public static ClassLoader getClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable ex) {
            // the next test for null will take care of any errors
        }
        if (cl == null) {
            // No thread context ClassLoader, use ClassLoader of this class
            cl = XQSync.class.getClassLoader();
        }
        if (cl == null) {
            cl = ClassLoader.getSystemClassLoader();
        }
        return cl;
    }

}
