/*
 * Copyright (c)2004-2009 Mark Logic Corporation
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
        configuration = _configuration;
        pool = _pool;
        factory = _factory;
        monitor = _monitor;
        queue = _queue;
        completionService = _cs;
        logger = configuration.getLogger();
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

            while (null != queue) {
                String uri = null;
                try {
                    uri = queue.poll(SLEEP_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.logException("interrupted", e);
                    if (null == uri) {
                        continue;
                    }
                }
                if (null == uri) {
                    logger.finer(this + " uri null, active " + active);
                    if (!active) {
                        // queue is empty
                        break;
                    }
                    continue;
                }
                if (0 == count) {
                    logger.fine("took first uri: " + uri);
                }
                logger.fine(count + ": uri = " + uri);
                buffer[bufferIndex] = uri;
                bufferIndex++;

                if (buffer.length == bufferIndex) {
                    logger.fine("submitting " + buffer.length);
                    completionService.submit(factory.newTask(buffer));
                    buffer = new String[buffer.length];
                    bufferIndex = 0;
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
            }

        } catch (SyncException e) {
            // stop the world
            logger.logException("fatal error", e);
            System.exit(1);
        }

        logger.fine("finished queuing " + count + " uris");
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
        queue.add(_uri);
        monitor.incrementTaskCount();
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
        return queue.size();
    }

    /**
     * @return
     */
    public boolean isActive() {
        return active;
    }

}
