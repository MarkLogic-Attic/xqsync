/*
 * Copyright 2003-2005 Mark Logic Corporation. All rights reserved.
 *
 */

package com.marklogic.ps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class PropertyManager extends Thread {

    static protected SimpleLogger logger = SimpleLogger.getSimpleLogger();

    protected File propertyFile;

    protected long lastModified;

    private static final long SLEEP_TIME = 500;

    private Properties properties;

    protected String propertyFilePath;

    protected PropertyClientInterface[] clients;

    /**
     * @param propertyFile
     * @throws IOException
     * @throws XDCFException
     */
    public PropertyManager(String _propertyFilePath) throws IOException {
        // set up to periodically check propertyFile for changes
        propertyFilePath = _propertyFilePath;
        propertyFile = new File(_propertyFilePath);
        lastModified = propertyFile.lastModified();
        properties = new Properties();
        reload();
    }

    /**
     * @param _propertyFile
     * @throws XDCFException
     */
    protected void reload() throws IOException {
        properties.load(new FileInputStream(propertyFilePath));
        logger.configureLogger(properties);
    }

    public void run() {
        long newLastModified;
        while (propertyFile != null && propertyFile.exists()
                && propertyFile.canRead()) {
            newLastModified = propertyFile.lastModified();
            // logger.finest("checking properties: " + newLastModified + " > " +
            // lastModified);
            if (newLastModified > lastModified) {
                logger.info("updating properties");
                try {
                    reload();
                    // each object in services must implement
                    // PropertyClientInterface
                    if (clients != null) {
                        for (int i = 0; i < clients.length; i++) {
                            if (clients[i] != null)
                                clients[i].setProperties(properties);
                        }
                    }
                    // hard-coded static classes
                    Connection.setProperties(properties);
                } catch (IOException e) {
                    logger.logException(propertyFilePath, e);
                }
                lastModified = newLastModified;
            }
            try {
                // sleep a little
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * @param services
     */
    public void addClients(PropertyClientInterface[] _clients) {
        clients = _clients;
    }

    /**
     * @param oo
     */
    public void add(PropertyClientInterface _client) {
        if (clients == null)
            clients = new PropertyClientInterface[] { _client };
        else {
            PropertyClientInterface[] newClients = new PropertyClientInterface[1 + clients.length];
            for (int i = 0; i < clients.length; i++) {
                newClients[i] = clients[i];
            }
            newClients[clients.length] = _client;
            clients = newClients;
        }
    }

    /**
     * 
     */
    public void quit() {
        propertyFile = null;
        try {
            this.notify();
        } catch (IllegalMonitorStateException e) {
            // ignore
        }
    }

}