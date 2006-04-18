/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps;

import java.util.Properties;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class AbstractLoggableClass {

    protected static SimpleLogger logger;

    private static Properties loggerProperties;

    public AbstractLoggableClass() {
        initialize();
    }

    protected static void initialize() {
        // lazy initialization
        if (logger == null) {
            logger = SimpleLogger.getSimpleLogger();
            if (loggerProperties != null) {
                logger.configureLogger(loggerProperties);
            }
        }
    }

    /**
     * @param _logger
     */
    public static void setLogger(SimpleLogger _logger) {
        logger = _logger;
        if (loggerProperties != null) {
            logger.configureLogger(loggerProperties);
        }
    }

    /**
     * @param _props
     */
    public static void setLoggerProperties(Properties _props) {
        loggerProperties = _props;
        if (logger != null) {
            logger.configureLogger(loggerProperties);
        }
    }

}
