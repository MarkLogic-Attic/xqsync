/*
 * Copyright (c)2004-2022 MarkLogic Corporation
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
import java.util.logging.Level;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class PropertyManager extends Thread {

    protected static final SimpleLogger logger = SimpleLogger.getSimpleLogger();
    protected File propertyFile;
    protected long lastModified;
    private static final long SLEEP_TIME = 500;
    private final Properties properties;
    protected final String propertyFilePath;
    protected PropertyClientInterface[] clients;

    /**
     *
     * @param propertyFilePath
     */
    public PropertyManager(String propertyFilePath) {
        // set up to periodically check propertyFile for changes
        this.propertyFilePath = propertyFilePath;
        propertyFile = new File(propertyFilePath);
        lastModified = propertyFile.lastModified();
        properties = new Properties();
        //reload();
    }

    /**
     *
     * @throws IOException
     */
    protected void reload() throws IOException {
        try (FileInputStream fileInputStream = new FileInputStream(propertyFilePath)) {
            properties.load(fileInputStream);
        }
        logger.configureLogger(properties);
    }

    @Override
    public void run() {
        long newLastModified;
        while (propertyFile != null && propertyFile.exists() && propertyFile.canRead()) {
            newLastModified = propertyFile.lastModified();
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("checking properties: " + newLastModified + " > " + lastModified);
            }
            if (newLastModified > lastModified) {
                logger.info("updating properties");
                try {
                    reload();
                    // each object in services must implement
                    // PropertyClientInterface
                    if (clients != null) {
                        for (PropertyClientInterface client : clients) {
                            if (client != null) {
                                client.setProperties(properties);
                            }
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
                // reset interrupt status and continue
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * @param clients
     */
    public void addClients(PropertyClientInterface[] clients) {
        this.clients = clients;
    }

    /**
     * @param client
     */
    public void add(PropertyClientInterface client) {
        if (clients == null) {
            clients = new PropertyClientInterface[]{client};
        } else {
            PropertyClientInterface[] newClients = new PropertyClientInterface[1 + clients.length];
            System.arraycopy(clients, 0, newClients, 0, clients.length);
            newClients[clients.length] = client;
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
