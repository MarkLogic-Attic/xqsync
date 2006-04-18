/*
 * Copyright 2003-2005 Mark Logic Corporation. All rights reserved.
 * 
 *  
 */
package com.marklogic.ps;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author Michael Blakeley michael.blakeley@marklogic.com
 * 
 * wrapper for java logging
 */
public class SimpleLogger extends Logger implements PropertyClientInterface {
	static public final String LOGGER_NAME = "com.marklogic.ps";

	private static Hashtable loggers = new Hashtable();

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

	public static synchronized SimpleLogger getSimpleLogger(String name) {
		SimpleLogger obj = (SimpleLogger) loggers.get(name);

		if (obj == null)
			obj = new SimpleLogger(name);

		return obj;
	}

	public static synchronized SimpleLogger getSimpleLogger(String name,
			String resBundle) {
		SimpleLogger obj = (SimpleLogger) loggers.get(name);

		if (obj == null)
			obj = new SimpleLogger(name, resBundle);

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
		// TODO support OS logging (via custom Handler?)
		//
		// don't use the root settings
		setUseParentHandlers(false);

		// now set the user's properties, if available
		String logLevel = _prop.getProperty("LOG_LEVEL", "INFO");

		// support multiple handlers: comma-separated
		String[] logHandler = _prop.getProperty("LOG_HANDLER", "CONSOLE,FILE")
				.split(",");
		String logFileProp = _prop.getProperty("LOG_FILEHANDLER_PATH",
				"simplelogger-%u-%g.log");
		String logFile = logFileProp;

		Handler h = null;
		if (logHandler != null && logHandler.length > 0) {
			// remove any old handlers
			Handler[] v = getHandlers();
			for (int i = 0; i < v.length; i++) {
				removeHandler(v[i]);
			}
			// can't use the logger here: all the handlers are gone!
			severe("this should not happen");
			for (int i = 0; i < logHandler.length; i++) {
				if (logHandler[i] == null)
					continue;
				// allow the user to specify the file
				if (logHandler[i].equals("FILE")) {
					System.err.println("logging to file " + logFile);
					try {
						h = new FileHandler(logFile);
					} catch (SecurityException e) {
						e.printStackTrace();
						// fatal error
						System.err.println("cannot configure logging: exiting");
						Runtime.getRuntime().exit(-1);
					} catch (IOException e) {
						e.printStackTrace();
						// fatal error
						System.err.println("cannot configure logging: exiting");
						Runtime.getRuntime().exit(-1);
					}
					h.setFormatter(new SimpleFormatter());
				} else if (logHandler[i].equals("CONSOLE")) {
					System.err.println("logging to " + logHandler[i]);
					h = new ConsoleHandler();
					h.setFormatter(new SimpleFormatter());
				} else {
                    // try to load the string as a classname
					try {
                        Class lhc = Class.forName( logHandler[i], true, ClassLoader.getSystemClassLoader() );
    					System.err.println("logging to class " + logHandler[i]);
                        Constructor con = lhc.getConstructor(new Class[] {});
                        h = (Handler) con.newInstance(new Object[] {});
                    } catch (Exception e) {
    					System.err.println("unrecognized LOG_HANDLER: "
    							+ logHandler[i]);
                        e.printStackTrace();
						System.err.println("cannot configure logging: exiting");
						Runtime.getRuntime().exit(-1);
                    }
				}
				if (h != null)
					addHandler(h);
			} // for handler properties
		} else {
			// default to ConsoleHandler
			h = new ConsoleHandler();
			h.setFormatter(new SimpleFormatter());
			addHandler(h);
		}

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
		info("setting up logging for: " + getName());
	} // setLogging

	public void logException(String message, Throwable exception) {
		if (message == null)
			message = "";
		super.log(Level.SEVERE, message, exception);
	} // logException

    /* (non-Javadoc)
     * @see com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties)
     */
    public void setProperties(Properties _properties) {
        configureLogger(_properties);
    }

}
