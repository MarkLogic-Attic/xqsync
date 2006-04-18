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
