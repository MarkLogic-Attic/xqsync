/**
 * Copyright (c)2006-2007 Mark Logic Corporation
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

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Monitor extends Thread {

    private static final int DISPLAY_MILLIS = 15 * 1000;

    private static final int SLEEP_MILLIS = 500;

    private static SimpleLogger logger;

    private ThreadPoolExecutor pool;

    private static long lastDisplayMillis = 0;

    String lastUri;

    private boolean running = true;

    private long eventCount = 0;

    private CompletionService<String> completionService;

    /**
     * @param _logger
     * @param _pool
     * @param _cs
     */
    public Monitor(SimpleLogger _logger, ThreadPoolExecutor _pool,
            CompletionService<String> _cs) {
        completionService = _cs;
        pool = _pool;
        logger = _logger;
    }

    public void run() {
        try {
            if (logger == null) {
                throw new NullPointerException("must call setLogger");
            }
            logger.fine("starting");
            monitor();
            logger.info("loaded " + eventCount + " records ok");
        } catch (Exception e) {
            if (e instanceof ExecutionException) {
                logger.logException("fatal execution error", e.getCause());
            } else {
                logger.logException("fatal error", e);
            }
            // stop the world
            System.exit(-1);
        } finally {
            pool.shutdownNow();
        }
        logger.fine("exiting");
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
    private void monitor() throws ExecutionException {
        int displayMillis = DISPLAY_MILLIS;
        int sleepMillis = SLEEP_MILLIS;
        Future<String> future = null;
        long currentMillis;

        // if anything goes wrong, the futuretask knows how to stop us
        long taskCount = pool.getTaskCount();
        logger.finest("looping every " + sleepMillis + ", core="
                + pool.getCorePoolSize() + ", active="
                + pool.getActiveCount() + ", tasks=" + taskCount);

        // run until all futures have been checked
        while (running && !pool.isTerminated()) {
            // try to avoid thread starvation
            yield();

            currentMillis = System.currentTimeMillis();
            if (currentMillis - lastDisplayMillis > displayMillis) {
                lastDisplayMillis = currentMillis;
                taskCount = pool.getTaskCount();
                logger.finer("thread count: core="
                        + pool.getCorePoolSize() + ", active="
                        + pool.getActiveCount() + ", tasks=" + taskCount);
                if (lastUri != null) {
                    logger.info("processed item " + eventCount + " of "
                            + taskCount + " as " + lastUri);
                }
            }

            // check completed tasks
            do {
                // try to avoid thread starvation
                yield();

                try {
                    future = completionService.poll(SLEEP_MILLIS,
                            TimeUnit.MILLISECONDS);
                    if (null != future) {
                        // check for exceptions and log result (or throw
                        // exception)
                        lastUri = future.get();
                        eventCount++;
                    }
                } catch (InterruptedException e) {
                    logger.logException("interrupted in poll() or get()",
                            e);
                    continue;
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
