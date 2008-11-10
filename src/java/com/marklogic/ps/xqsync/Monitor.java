/**
 * Copyright (c)2006-2008 Mark Logic Corporation
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

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.ps.timing.Timer;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Monitor extends Thread {

    protected static final int DISPLAY_MILLIS = 60 * 1000;

    protected static final int SLEEP_MILLIS = 500;

    protected static SimpleLogger logger;

    protected ThreadPoolExecutor pool;

    protected static long lastDisplayMillis = 0;

    protected boolean running = true;

    protected CompletionService<TimedEvent> completionService;

    protected boolean fatalErrors = Configuration.FATAL_ERRORS_DEFAULT_BOOLEAN;

    protected Timer timer;

    protected long taskCount;

    /**
     * @param _logger
     * @param _pool
     * @param _cs
     * @param _fatalErrors
     */
    public Monitor(SimpleLogger _logger, ThreadPoolExecutor _pool,
            CompletionService<TimedEvent> _cs, boolean _fatalErrors) {
        completionService = _cs;
        pool = _pool;
        logger = _logger;
        fatalErrors = _fatalErrors;
    }

    public void run() {
        try {
            if (logger == null) {
                throw new NullPointerException("must call setLogger");
            }
            logger.info("starting");
            monitor();
        } catch (Exception e) {
            if (e instanceof ExecutionException) {
                logger
                        .logException("fatal execution error", e
                                .getCause());
            } else {
                logger.logException("fatal error", e);
            }
            // stop the world
            System.exit(-1);
        } finally {
            pool.shutdownNow();
        }
        logger.info("exiting after " + timer.getEventCount() + "/"
                + taskCount + ", " + timer.getProgressMessage());
    }

    /**
     * 
     */
    public void halt(Throwable t) {
        logger.logException("halting", t);
        running = false;
        pool.shutdownNow();
    }

    /**
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     */
    protected void monitor() throws ExecutionException {
        int displayMillis = DISPLAY_MILLIS;
        int sleepMillis = SLEEP_MILLIS;
        Future<TimedEvent> future = null;
        long currentMillis;
        TimedEvent lastEvent = null;

        taskCount = pool.getTaskCount();
        logger.finest("looping every " + sleepMillis + ", core="
                + pool.getCorePoolSize() + ", active="
                + pool.getActiveCount() + ", tasks=" + taskCount);

        timer = new Timer();

        // run until all futures have been checked
        while (running && !pool.isTerminated()) {
            // try to avoid thread starvation
            yield();

            // check completed tasks
            // sometimes this goes so fast that we would never leave the loop,
            // so progress is never displayed... so limit the number of loops.
            do {
                try {
                    future = completionService.poll(SLEEP_MILLIS,
                            TimeUnit.MILLISECONDS);
                    if (null != future) {
                        // record result, or throw exception
                        try {
                            lastEvent = future.get();
                            if (null != lastEvent) {
                                // discard events to reduce memory utilization
                                timer.add(lastEvent, false);
                            } else {
                                // special - queuing is complete
                                // accept no new tasks
                                logger.fine("shutting down pool");
                                pool.shutdown();
                            }
                        } catch (ExecutionException e) {
                            if (fatalErrors) {
                                throw e;
                            }
                            logger.logException("non-fatal", e);
                            timer.incrementEventCount(false);
                        }
                    }
                } catch (InterruptedException e) {
                    logger.logException("interrupted in poll() or get()",
                            e);
                    continue;
                }

                currentMillis = System.currentTimeMillis();
                if (currentMillis - lastDisplayMillis > displayMillis) {
                    lastDisplayMillis = currentMillis;
                    taskCount = pool.getTaskCount();
                    logger.finer("thread count: core="
                            + pool.getCorePoolSize() + ", active="
                            + pool.getActiveCount() + ", tasks="
                            + taskCount);
                    if (lastEvent != null) {
                        logger.info("" + timer.getEventCount() + "/"
                                + taskCount + ", "
                                + timer.getProgressMessage() + ", "
                                + lastEvent.getDescription());
                    }
                }

            } while (null != future);

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

}
