/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2004-2010 Mark Logic Corporation
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
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class UriQueue extends Thread {

    protected static final long SLEEP_MILLIS = 125;

    protected Configuration configuration;

    protected volatile BlockingQueue<String> queue;

    protected TaskFactory factory;

    protected CompletionService<TimedEvent[]> completionService;

    protected boolean active;

    protected ThreadPoolExecutor pool;

    protected SimpleLogger logger;

    protected Monitor monitor;

    protected boolean useQueueFile = false;

    protected File queueFile;

    protected PrintWriter queueFileWriter;

    protected BufferedReader queueFileReader;

    protected int queueFileEntries = 0;

    protected Object queueFileMutex = new Object();


    /**
     * @param _configuration
     * @param _cs
     * @param _pool
     * @param _factory
     * @param _monitor
     * @param _queue
     */
    public UriQueue(Configuration _configuration,
            CompletionService<TimedEvent[]> _cs,
            ThreadPoolExecutor _pool, TaskFactory _factory,
            Monitor _monitor, BlockingQueue<String> _queue) {
        super("UriQueueThread");
        configuration = _configuration;
        pool = _pool;
        factory = _factory;
        monitor = _monitor;
        queue = _queue;
        completionService = _cs;
        logger = configuration.getLogger();
        useQueueFile = configuration.useQueueFile();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
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
                    yield();
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
                yield();
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
     * @param _uri
     */
    public void add(String _uri) {
        yield();
        synchronized (queueFileMutex) {

            if (!useQueueFile)
                queue.add(_uri);
            else { 
                addUriToFile(_uri);
                queueFileEntries++;
            }

            monitor.incrementTaskCount();
        }
        yield();
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
    private void addUriToFile(String _uri) {

        if (null == queueFile) {
            try {
                
                if (configuration.getUriQueueFile() != null) {
                    queueFile = new File(configuration.getUriQueueFile());
                    if (queueFile.exists())
                        queueFile.delete();
                    queueFile.createNewFile();
                } else if (configuration.getTmpDir() != null) {
                    queueFile = File.createTempFile("xqsync", ".txt", 
                                                    new File(configuration.getTmpDir()));
                } else {
                    queueFile = File.createTempFile("xqsync", ".txt");
                }

                if (!configuration.keepUriQueueFile())
                    queueFile.deleteOnExit();

                queueFileWriter = new PrintWriter(queueFile);
                queueFileReader = new BufferedReader(new FileReader(queueFile));
            } catch (Exception e) {
                // stop the world
                logger.logException("fatal error", e);
                System.exit(1);
            } 
        }

        queueFileWriter.println(_uri);
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
