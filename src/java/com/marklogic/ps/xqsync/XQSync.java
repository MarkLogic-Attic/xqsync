/*
 * Copyright (c)2004-2006 Mark Logic Corporation
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.marklogic.ps.AbstractLoggableClass;
import com.marklogic.ps.PropertyManager;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSync extends AbstractLoggableClass {

    public static String VERSION = "2006-08-05.1";

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        // assume that any input files are properties
        Properties props = new Properties();
        PropertyManager[] pmArray = new PropertyManager[args.length];
        for (int i = 0; i < args.length; i++) {
            props.load(new FileInputStream(args[i]));
            pmArray[i] = new PropertyManager(args[i]);
            pmArray[i].start();
            System.err.println("loaded properties from " + args[i]);
        }

        // allow system properties to override
        props.putAll(System.getProperties());

        AbstractLoggableClass.setLoggerProperties(props);
        AbstractLoggableClass.initialize();

        logger.info("XQSync starting: version = " + VERSION);

        // TODO set and use INPUT_ENCODING and OUTPUT_ENCODING, instead
        logger.info("default encoding is "
                + System.getProperty("file.encoding"));
        System.setProperty("file.encoding", props.getProperty(
                "DEFAULT_CHARSET", "UTF-8"));
        logger.info("default encoding is now "
                + System.getProperty("file.encoding"));

        long start = System.currentTimeMillis();

        XQSyncManager xqm = new XQSyncManager(logger, props);
        xqm.start();
        while (xqm.isAlive()) {
            try {
                xqm.join();
            } catch (InterruptedException e) {
                logger.logException("interrupted in join", e);
            }
        }

        logger.info("synchronized in "
                + (System.currentTimeMillis() - start) + " ms");
        for (int i = 0; i < pmArray.length; i++) {
            pmArray[i].quit();
        }
    }
}
