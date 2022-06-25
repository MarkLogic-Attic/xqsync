/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2005-2022 MarkLogic Corporation
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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.Formatter;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 * wrapper for java logging
 */
public class SimpleLogger extends Logger implements PropertyClientInterface {
    public static final String LOG_FILEHANDLER_LIMIT = "LOG_FILEHANDLER_LIMIT";
    public static final String LOG_FILEHANDLER_COUNT = "LOG_FILEHANDLER_COUNT";
    public static final String LOG_FILEHANDLER_APPEND = "LOG_FILEHANDLER_APPEND";
    public static final String LOG_FILEHANDLER_PATH = "LOG_FILEHANDLER_PATH";
    public static final String DEFAULT_LOG_HANDLER = "CONSOLE,FILE";
    public static final String DEFAULT_LOG_LEVEL = "INFO";
    public static final String LOG_HANDLER = "LOG_HANDLER";
    public static final String LOG_LEVEL = "LOG_LEVEL";
    public static final String DEFAULT_FILEHANDLER_PATH = "simplelogger-%u-%g.log";
    public static final String LOGGER_NAME = "com.marklogic.ps";
    public static final String LOG_FORMATTER = "LOG_FORMATTER";
    private static final Map<String, SimpleLogger> loggers = Collections.synchronizedMap(new Hashtable<>());

    SimpleLogger(String name) {
        super(name, null);
        loggers.put(name, this);
        this.setParent(Logger.getLogger(""));
    }

    SimpleLogger(String name, String resBundle) {
        super(name, resBundle);
        loggers.put(name, this);
        this.setParent(Logger.getLogger(""));
    }

    public static SimpleLogger getSimpleLogger() {
        return getSimpleLogger(LOGGER_NAME);
    }

    public static SimpleLogger getSimpleLogger(String name) {
        return getSimpleLogger(name, null);
    }

    public static SimpleLogger getSimpleLogger(String name,
            String resBundle) {
        SimpleLogger obj = loggers.get(name);

        if (obj == null) {
            if (resBundle != null) {
                obj = new SimpleLogger(name, resBundle);
            } else {
                obj = new SimpleLogger(name);
            }
        }

        return obj;
    }

    public void configureLogger(Properties prop) {
        if (prop == null) {
            System.err.println("WARNING: null properties. Cannot configure logger");
            return;
        }

        /*
         * set up logging: we want to use the properties to set up all logging
         * thus, we need to use "com.marklogic.ps" as our logger. Note that
         * getParent() appears to fetch the first non-null ancestor, usually
         * root! So we take a cruder approach.
         */

        // don't use the root settings
        setUseParentHandlers(false);

        // now set the user's properties, if available
        String logLevel = prop.getProperty(LOG_LEVEL, DEFAULT_LOG_LEVEL);

        // support multiple handlers: comma-separated
        String[] newHandlers = prop.getProperty(LOG_HANDLER, DEFAULT_LOG_HANDLER).split(",");
        String logFilePath = prop.getProperty(LOG_FILEHANDLER_PATH, DEFAULT_FILEHANDLER_PATH);
        boolean logFileAppend = Boolean.parseBoolean(prop.getProperty(LOG_FILEHANDLER_APPEND, "true"));
        int logFileCount = Integer.parseInt(prop.getProperty(LOG_FILEHANDLER_COUNT, "1"));
        int logFileLimit = Integer.parseInt(prop.getProperty(LOG_FILEHANDLER_LIMIT, "0"));
        String logFormatter = prop.getProperty(LOG_FORMATTER, null);

        Handler handler = null;
        if (newHandlers != null && newHandlers.length > 0) {
            // remove any old handlers
            Handler[] oldHandlers = getHandlers();
            int size = oldHandlers.length;
            if (size < newHandlers.length) {
                size = newHandlers.length;
            }

            for (int i = 0; i < size; i++) {
                if (i >= newHandlers.length) {
                    // nothing to do except remove the old one
                    removeHandler(oldHandlers[i]);
                    continue;
                }

                // System.err.println("new handler " + i + ": "
                // + newHandlers[i]);
                if (i < oldHandlers.length && oldHandlers[i] != null) {
                    // System.err.println("old handler " + i + ": "
                    // + oldHandlers[i].getClass().getSimpleName());
                    if (newHandlers[i].equals("CONSOLE")
                            && oldHandlers[i] instanceof ConsoleHandler) {
                        continue;
                    } else if (newHandlers[i].equals("FILE")
                            && oldHandlers[i] instanceof FileHandler) {
                        /*
                         * This is a hack: we don't know that the file pattern
                         * is the same, but FileHandler doesn't seem to give us
                         * any help with that. So changing the pattern won't
                         * work.
                         */
                        continue;
                    } else if (newHandlers[i].equals(oldHandlers[i]
                            .getClass().getSimpleName())) {
                        continue;
                    }
                }

                // remove the old handler
                if (i < oldHandlers.length) {
                    // System.err.println("removing " + i + ": "
                    // + oldHandlers[i]);
                    removeHandler(oldHandlers[i]);
                }

                // allow the user to specify the file
                if (newHandlers[i].equals("FILE")) {
                    System.err.println("logging to file " + logFilePath);
                    try {
                        handler = new FileHandler(logFilePath, logFileLimit, logFileCount, logFileAppend);
                    } catch (SecurityException | IOException e) {
                        e.printStackTrace();
                        // fatal error
                        System.err.println("cannot configure logging: exiting");
                        Runtime.getRuntime().exit(-1);
                    }
                } else if (newHandlers[i].equals("CONSOLE")) {
                    System.err.println("logging to " + newHandlers[i]);
                    handler = new ConsoleHandler();
                } else {
                    // try to load the string as a classname
                    try {
                        Class<? extends Handler> lhc = Class.forName(
                                newHandlers[i], true,
                                ClassLoader.getSystemClassLoader())
                                .asSubclass(Handler.class);
                        System.err.println("logging to class " + newHandlers[i]);
                        Constructor<? extends Handler> con = lhc.getConstructor();
                        handler = con.newInstance();
                    } catch (Exception e) {
                        System.err.println("unrecognized LOG_HANDLER: " + newHandlers[i]);
                        e.printStackTrace();
                        System.err.println("cannot configure logging: exiting");
                        Runtime.getRuntime().exit(-1);
                    }
                }
                if (handler != null)
                    addHandler(handler);
            } // for handler properties
        } else {
            // default to ConsoleHandler
            handler = new ConsoleHandler();
            addHandler(handler);
        }

        // set logging level for all handers
        if (logLevel != null) {
            /*
             * Logger.setLevel() isn't sufficient, unless the Handler.level is
             * set equal or lower
             */
            Level level = Level.parse(logLevel);
            if (level != null) {
                setLevel(level);
                for (Handler _handler : getHandlers()) {
                    _handler.setLevel(level);
                }
            }
            fine("logging set to " + getLevel());
        }

        // set formatter for all handlers
        for (Handler _handler : getHandlers()) {
            Formatter f = null;
            // use OneLineFormatter if specified
            if ("SimpleFormatter".equals(logFormatter)) {
                f = new SimpleFormatter();
            }
            // use SimpleFormatter as the default
            if (null == logFormatter) {
                f = new OneLineFormatter();
            }
            _handler.setFormatter(f);
        }

        info("setting up " + this + " for: " + getName());
    } // setLogging

    public void logException(String message, Throwable exception) {
        if (message == null) {
            message = "";
        }
        super.log(Level.SEVERE, message, exception);
    } // logException

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties)
     */
    public void setProperties(Properties properties) {
        configureLogger(properties);
    }

}
