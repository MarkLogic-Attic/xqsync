/*
 * Copyright (c)2004-2009 Mark Logic Corporation
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
import java.util.Properties;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.xcc.Version;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSync {

    public static String VERSION = "2009-07-15.1";

    private static String versionMessage = "version " + VERSION + " on "
            + System.getProperty("java.version") + " ("
            + System.getProperty("java.runtime.name") + ")";

    public static void main(String[] args) throws Exception {

        // make sure the environment is healthy
        String encoding = System.getProperty("file.encoding");
        if (!encoding.equals("UTF-8")) {
            throw new IOException(
                    "UTF-8 encoding is required: System property file.encoding "
                            + encoding
                            + " is not UTF-8. "
                            + "Change your locale, or set -Dfile.encoding=UTF-8");
        }

        // assume that any input files are properties
        Properties props = new Properties();
        for (int i = 0; i < args.length; i++) {
            props.load(new FileInputStream(args[i]));
            System.err.println("loaded properties from " + args[i]);
        }

        // allow system properties to override
        props.putAll(System.getProperties());
        System.err.println("added system properties");

        SimpleLogger logger = SimpleLogger.getSimpleLogger();
        logger.configureLogger(props);

        logger.info("XQSync starting: " + versionMessage);
        logger.info("XCC version = " + Version.getVersionString());

        Configuration configuration = new Configuration();
        configuration.setLogger(logger);
        configuration.setProperties(props);

        // repeat startup info to log - and for emphasis
        logger.info("XQSync starting: " + versionMessage);
        logger.info("XCC version = " + Version.getVersionString());

        long start = System.currentTimeMillis();

        // we don't need the manager to be a Thread, so run it directly
        XQSyncManager xqm = new XQSyncManager(configuration);
        xqm.run();

        long duration = System.currentTimeMillis() - start;
        long itemsQueued = xqm.getItemsQueued();
        logger.info("completed " + itemsQueued + " in " + duration
                + " ms (" + (int) (1000 * itemsQueued / duration)
                + " docs/s)");
    }
}
