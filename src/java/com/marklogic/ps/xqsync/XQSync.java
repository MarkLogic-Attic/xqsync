/*
 * Copyright 2005-2006 Mark Logic Corporation. All rights reserved.
 *
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
    
    public static String VERSION = "2006-04-14";

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

        logger.configureLogger(props);
        logger.info("XQSync starting: version = " + VERSION);
        
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
        
        logger.info("synchronized in " + (System.currentTimeMillis() - start) + " ms");
        for (int i=0; i < pmArray.length; i++) {
            pmArray[i].quit();
        }
    }
}