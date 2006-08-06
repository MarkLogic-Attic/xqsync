/**
 * Copyright (c)2006 Mark Logic Corporation
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Monitor extends Thread {
    private static final int DISPLAY_MILLIS = 3 * 1000;

    private static final int SLEEP_TIME = 500;

    private static SimpleLogger logger;

    private ThreadPoolExecutor pool;

    private static long lastDisplayMillis = 0;

    String lastUri;

    private boolean running = true;

    private long eventCount = 0;

    private ExecutorCompletionService completionService;

    private long numberOfTasks = -1;

    public void run() {
        try {
            if (logger == null) {
                throw new NullPointerException("must call setLogger");
            }
            logger.fine("starting");
            monitor();
            logger.info("loaded " + eventCount + " records ok");
        } catch (Exception e) {
            logger.logException("fatal error", e);
        } finally {
            pool.shutdownNow();
        }
        logger.fine("exiting");
    }

    /**
     * 
     */
    public void halt() {
        logger.info("halting");
        running = false;
        pool.shutdownNow();
        interrupt();
    }

    /**
     * @throws ExecutionException 
     * @throws InterruptedException 
     * 
     */
    private void monitor() throws InterruptedException, ExecutionException {
        int displayMillis = DISPLAY_MILLIS;
        int sleepMillis = SLEEP_TIME;
        Future future;
        long currentMillis;

        // if anything goes wrong, the futuretask knows how to stop us
        logger.finest("looping every " + sleepMillis + ", core="
                + pool.getCorePoolSize() + ", active="
                + pool.getActiveCount() + ", tasks="
                + pool.getTaskCount());
        while (running && !isInterrupted()) {
            currentMillis = System.currentTimeMillis();
            if (currentMillis - lastDisplayMillis > displayMillis) {
                lastDisplayMillis = currentMillis;
                logger.finer("thread count: core="
                        + pool.getCorePoolSize() + ", active="
                        + pool.getActiveCount() + ", tasks="
                        + pool.getTaskCount());
                if (lastUri != null) {
                    logger.info("processed item " + eventCount + " as "
                            + lastUri);
                }
            }
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                logger.logException("sleep was interrupted: continuing",
                        e);
            }
            
            // check completed tasks
            if (pool.getCompletedTaskCount() > 0) {
                future = completionService.take();
                // check for exceptions and log result (or throw exception)
                lastUri = (String) future.get();
                eventCount = pool.getCompletedTaskCount();
            }

            if (numberOfTasks > -1 && pool.getCompletedTaskCount() >= numberOfTasks) {
                logger
                        .fine("stopping because there are no active threads");
                break;
            }
        }
    }

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

    /**
     * @param _pool
     */
    public void setPool(ThreadPoolExecutor _pool) {
        pool = _pool;
    }

    /**
     * @param completionService
     */
    public void setTasks(ExecutorCompletionService completionService) {
        this.completionService = completionService;
    }

    /**
     * @param numberOfTasks
     */
    public void setNumberOfTasks(long numberOfTasks) {
        this.numberOfTasks = numberOfTasks;
    }

}
