/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
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
package com.marklogic.ps.xqsync;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class UriQueue extends Thread {

    protected static final long SLEEP_MILLIS = 125;
    protected final Configuration configuration;
    protected volatile BlockingQueue<String> queue;
    protected final TaskFactory factory;
    protected final CompletionService<TimedEvent[]> completionService;
    protected boolean active;
    protected final ThreadPoolExecutor pool;
    protected final SimpleLogger logger;
    protected final Monitor monitor;
    protected boolean useQueueFile = false;
    protected File queueFile;
    protected PrintWriter queueFileWriter;
    protected BufferedReader queueFileReader;
    protected int queueFileEntries = 0;
    protected final Object queueFileMutex = new Object();


    /**
     * @param configuration
     * @param cs
     * @param pool
     * @param factory
     * @param monitor
     * @param queue
     */
    public UriQueue(Configuration configuration, CompletionService<TimedEvent[]> cs, ThreadPoolExecutor pool, TaskFactory factory, Monitor monitor, BlockingQueue<String> queue) {
        super("UriQueueThread");
        this.configuration = configuration;
        this.pool = pool;
        this.factory = factory;
        this.monitor = monitor;
        this.queue = queue;
        completionService = cs;
        logger = configuration.getLogger();
        useQueueFile = configuration.useQueueFile();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        active = true;
        long count = 0;
        SimpleLogger logger = configuration.getLogger();

        String[] buffer = new String[configuration.getInputBatchSize()];
        int bufferIndex = 0;

        try {
            if (null == factory) {
                throw new SyncException("null factory");
            }
            if (null == completionService) {
                throw new SyncException("null completion service");
            }

            while (true) {

                String uri = null;

                if (useQueueFile) {
                    uri = getUriFromFile();
                } else {
                    try {
                        uri = queue.poll(SLEEP_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // reset interrupt status and continue
                        Thread.interrupted();
                        logger.logException("interrupted", e);
                        Thread.currentThread().interrupt();
                        if (null == uri) {
                            continue;
                        }
                    }
                }

                if (null == uri) {
                    // The in-memory queue returned with nothing
                    logger.finer(this + " uri null, active " + active);

                    if (!active) {
                        // queue is empty
                        break;
                    } else if (useQueueFile) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            // do nothing
                            Thread.currentThread().interrupt();
                        }
                    }

                    continue;
                }

                if (0 == count) {
                    logger.finest("took first uri: " + uri);
                }

                logger.finest(count + ": uri = " + uri);
                buffer[bufferIndex] = uri;
                bufferIndex++;

                if (buffer.length == bufferIndex) {
                    logger.finest("submitting " + buffer.length);
                    completionService.submit(factory.newTask(buffer));
                    buffer = new String[buffer.length];
                    bufferIndex = 0;
                    Thread.yield();
                }

                count++;
            }

            // handle any buffered uris
            logger.fine("cleaning up " + bufferIndex);
            if (bufferIndex > 0) {
                // make sure we don't queue anything twice
                for (int i = bufferIndex; i < buffer.length; i++) {
                    buffer[i] = null;
                }
                completionService.submit(factory.newTask(buffer));
                Thread.yield();
            }

        } catch (SyncException e) {
            // stop the world
            logger.logException("fatal error", e);
            System.exit(1);
        }

        logger.finest("finished queuing " + count + " uris");
    }

    public synchronized void shutdown() {
        // ignore multiple calls
        if (active) {
            logger.fine("closing queue " + this);
        }
        // graceful shutdown, draining the queue
        active = false;
    }

    /**
     *
     */
    public void halt() {
        // something bad happened - make sure we exit the loop
        logger.info("halting queue");
        queue = null;
        active = false;
        pool.shutdownNow();
        interrupt();
    }

    /**
     * @param uri
     */
    public void add(String uri) {
        Thread.yield();
        synchronized (queueFileMutex) {

            if (!useQueueFile)
                queue.add(uri);
            else { 
                addUriToFile(uri);
                queueFileEntries++;
            }

            monitor.incrementTaskCount();
        }
        Thread.yield();
    }

    /**
     * @return
     */
    public CompletionService<TimedEvent[]> getCompletionService() {
        return completionService;
    }

    /**
     * @return
     */
    public ThreadPoolExecutor getPool() {
        return pool;
    }

    /**
     * @return
     */
    public Monitor getMonitor() {
        return monitor;
    }

    /**
     * @return
     */
    public int getQueueSize() {
        if (useQueueFile)
            return queueFileEntries;
        else 
            return queue.size();
    }
    
    /**
     * @return
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Add the uri to a temporary file
     */
    private void addUriToFile(String uri) {

        if (null == queueFile) {
            try {
                
                if (configuration.getUriQueueFile() != null) {
                    queueFile = new File(configuration.getUriQueueFile());
                    if (queueFile.exists()) {
                        queueFile.delete();
                    }
                    queueFile.createNewFile();
                } else if (configuration.getTmpDir() != null) {
                    queueFile = File.createTempFile("xqsync", ".txt", new File(configuration.getTmpDir()));
                } else {
                    queueFile = File.createTempFile("xqsync", ".txt");
                }

                if (!configuration.keepUriQueueFile()) {
                    queueFile.deleteOnExit();
                }
                queueFileWriter = new PrintWriter(queueFile);
                queueFileReader = new BufferedReader(new FileReader(queueFile));
            } catch (Exception e) {
                // stop the world
                logger.logException("fatal error", e);
                System.exit(1);
            } 
        }

        queueFileWriter.println(uri);
        queueFileWriter.flush();
    }

    /**
     * @returns the uri from the queue file, or null if error or EOF
     */
    private String getUriFromFile() {

        String uri = null;

        synchronized (queueFileMutex) {
            try {
                if (queueFileReader != null)
                    uri = queueFileReader.readLine();    

                if (null != uri)
                    queueFileEntries--;
            } catch (IOException e) {
                logger.logException("can't read uri queue file", e);
            }
        }

        return uri;
    }
}
