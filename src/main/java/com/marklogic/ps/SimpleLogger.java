/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2005-2017 MarkLogic Corporation
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
public class SimpleLogger extends Logger implements
        PropertyClientInterface {
    /**
     * 
     */
    static public final String LOG_FILEHANDLER_LIMIT = "LOG_FILEHANDLER_LIMIT";

    /**
     * 
     */
    static public final String LOG_FILEHANDLER_COUNT = "LOG_FILEHANDLER_COUNT";

    /**
     * 
     */
    static public final String LOG_FILEHANDLER_APPEND = "LOG_FILEHANDLER_APPEND";

    /**
     * 
     */
    static public final String LOG_FILEHANDLER_PATH = "LOG_FILEHANDLER_PATH";

    /**
     * 
     */
    static public final String DEFAULT_LOG_HANDLER = "CONSOLE,FILE";

    /**
     * 
     */
    static public final String DEFAULT_LOG_LEVEL = "INFO";

    /**
     * 
     */
    static public final String LOG_HANDLER = "LOG_HANDLER";

    /**
     * 
     */
    static public final String LOG_LEVEL = "LOG_LEVEL";

    /**
     * 
     */
    static public final String DEFAULT_FILEHANDLER_PATH = "simplelogger-%u-%g.log";

    static public final String LOGGER_NAME = "com.marklogic.ps";

    static public final String LOG_FORMATTER = "LOG_FORMATTER";

    private static Map<String, SimpleLogger> loggers = Collections
            .synchronizedMap(new Hashtable<String, SimpleLogger>());

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

    public void configureLogger(Properties _prop) {
        if (_prop == null) {
            System.err
                    .println("WARNING: null properties. Cannot configure logger");
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
        String logLevel = _prop.getProperty(LOG_LEVEL, DEFAULT_LOG_LEVEL);

        // support multiple handlers: comma-separated
        String[] newHandlers = _prop.getProperty(LOG_HANDLER,
                DEFAULT_LOG_HANDLER).split(",");
        String logFilePath = _prop.getProperty(LOG_FILEHANDLER_PATH,
                DEFAULT_FILEHANDLER_PATH);
        boolean logFileAppend = Boolean.valueOf(
                _prop.getProperty(LOG_FILEHANDLER_APPEND, "true"))
                .booleanValue();
        int logFileCount = Integer.parseInt(_prop.getProperty(
                LOG_FILEHANDLER_COUNT, "1"));
        int logFileLimit = Integer.parseInt(_prop.getProperty(
                LOG_FILEHANDLER_LIMIT, "0"));
        String logFormatter = _prop.getProperty(LOG_FORMATTER, null);

        Handler h = null;
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
                        h = new FileHandler(logFilePath, logFileLimit,
                                logFileCount, logFileAppend);
                    } catch (SecurityException e) {
                        e.printStackTrace();
                        // fatal error
                        System.err
                                .println("cannot configure logging: exiting");
                        Runtime.getRuntime().exit(-1);
                    } catch (IOException e) {
                        e.printStackTrace();
                        // fatal error
                        System.err
                                .println("cannot configure logging: exiting");
                        Runtime.getRuntime().exit(-1);
                    }
                } else if (newHandlers[i].equals("CONSOLE")) {
                    System.err.println("logging to " + newHandlers[i]);
                    h = new ConsoleHandler();
                } else {
                    // try to load the string as a classname
                    try {
                        Class<? extends Handler> lhc = Class.forName(
                                newHandlers[i], true,
                                ClassLoader.getSystemClassLoader())
                                .asSubclass(Handler.class);
                        System.err.println("logging to class "
                                + newHandlers[i]);
                        Constructor<? extends Handler> con = lhc
                                .getConstructor(new Class[] {});
                        h = con.newInstance(new Object[] {});
                    } catch (Exception e) {
                        System.err.println("unrecognized LOG_HANDLER: "
                                + newHandlers[i]);
                        e.printStackTrace();
                        System.err
                                .println("cannot configure logging: exiting");
                        Runtime.getRuntime().exit(-1);
                    }
                }
                if (h != null)
                    addHandler(h);
            } // for handler properties
        } else {
            // default to ConsoleHandler
            h = new ConsoleHandler();
            addHandler(h);
        }

        // set logging level for all handers
        if (logLevel != null) {
            /*
             * Logger.setLevel() isn't sufficient, unless the Handler.level is
             * set equal or lower
             */
            Level lvl = Level.parse(logLevel);
            if (lvl != null) {
                setLevel(lvl);
                Handler[] v = getHandlers();
                for (int i = 0; i < v.length; i++) {
                    v[i].setLevel(lvl);
                }
            }
            fine("logging set to " + getLevel());
        }

        // set formatter for all handlers
        Handler[] v = getHandlers();
        for (int i = 0; i < v.length; i++) {
            Formatter f = null;
            
            // use OneLineFormatter if specified
            if ("SimpleFormatter".equals(logFormatter))
                f = new SimpleFormatter();

            // use SimpleFormatter as the default
            if (null == logFormatter)
                f = new OneLineFormatter();

            v[i].setFormatter(f);
        }

        info("setting up " + this + " for: " + getName());
    } // setLogging

    public void logException(String message, Throwable exception) {
        if (message == null)
            message = "";
        super.log(Level.SEVERE, message, exception);
    } // logException

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties)
     */
    public void setProperties(Properties _properties) {
        configureLogger(_properties);
    }

}
