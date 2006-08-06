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
     * @throws XDCFException
     */
    public PropertyManager(String _propertyFilePath) {
        // set up to periodically check propertyFile for changes
        propertyFilePath = _propertyFilePath;
        propertyFile = new File(_propertyFilePath);
        lastModified = propertyFile.lastModified();
        properties = new Properties();
        //reload();
    }

    /**
     * @param _propertyFile
     * @throws IOException
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
                } catch (IOException e) {
                    logger.logException(propertyFilePath, e);
                }
                lastModified = newLastModified;
            }
            try {
                // sleep a little
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                // big deal
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
