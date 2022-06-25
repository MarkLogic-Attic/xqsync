/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * (c)2006-2022 MarkLogic Corporation
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
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class Monitor extends Thread {

    // TODO make these constants configurable?
    protected static final int DISPLAY_MILLIS = 60 * 1000;
    protected static final int FUTURE_MILLIS = 15 * 60 * 1000;
    protected static final int SLEEP_MILLIS = 500;
    protected SimpleLogger logger;
    protected boolean running = true;
    protected ThreadPoolExecutor pool;
    protected final CompletionService<TimedEvent[]> completionService;
    protected boolean fatalErrors = Configuration.FATAL_ERRORS_DEFAULT_BOOLEAN;
    protected Timer timer;
    protected long taskCount = 0;
    protected boolean taskCountFinal = false;
    protected final Object taskCountMutex = new Object();
    protected final Configuration config;

    /**
     * @param config
     * @param pool
     * @param cs
     * @param fatalErrors
     */
    public Monitor(Configuration config, ThreadPoolExecutor pool, CompletionService<TimedEvent[]> cs, boolean fatalErrors) {
        super("MonitorThread");
        this.config = config;
        completionService = cs;
        this.pool = pool;
        logger = config.getLogger();
        this.fatalErrors = fatalErrors;
    }

    @Override
    public void run() {
        if (null == logger) {
            throw new NullPointerException("must call setLogger");
        }
        try {
            logger.info("starting");
            monitor();
            Thread.yield();
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
            running = false;
            logger.info("exiting after " + timer.getEventCount() + "/" + taskCount + ", " + timer.getProgressMessage());
        }
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
     *
     */
    protected void monitor() throws ExecutionException {
        int futureMillis = FUTURE_MILLIS;
        Future<TimedEvent[]> future = null;
        /* Initialize lastFutureMillis so that we do not get
         * warnings on slow queue startup.
         */
        long currentMillis = System.currentTimeMillis();
        long lastDisplayMillis = 0;
        long lastFutureMillis = currentMillis;
        TimedEvent[] lastEvent = null;

        logger.finest("looping every " + SLEEP_MILLIS + ", core="
                + pool.getCorePoolSize() + ", active="
                + pool.getActiveCount() + ", tasks=" + taskCount);

        timer = new Timer();

        // run until all futures have been checked
        do {
            // try to avoid thread starvation
            Thread.yield();

            // check completed tasks
            // sometimes this goes so fast that we never leave the loop,
            // so progress is never displayed... so limit the number of loops.
            do {
                try {
                    future = completionService.poll(SLEEP_MILLIS, TimeUnit.MILLISECONDS);
                    if (null != future) {
                        // record result, or throw exception
                        lastFutureMillis = System.currentTimeMillis();
                        try {
                            lastEvent = future.get();
                            if (null == lastEvent) {
                                throw new FatalException("unexpected null event");
                            }
                            for (TimedEvent timedEvent : lastEvent) {
                                // discard events to reduce memory utilization
                                if (null != timedEvent) {
                                    timer.add(timedEvent, false);
                                }
                            }
                        } catch (ExecutionException e) {
                            if (fatalErrors) {
                                throw e;
                            }
                            Throwable cause = e.getCause();
                            if (cause instanceof FatalException) {
                                throw (FatalException) cause;
                            }
                            logger.logException("non-fatal", e);
                            timer.incrementEventCount(false);
                        }
                    }
                } catch (InterruptedException e) {
                    // reset interrupt status and continue
                    Thread.interrupted();
                    logger.logException("interrupted in poll() or get()", e);
                    Thread.currentThread().interrupt();
                    continue;
                }

                currentMillis = System.currentTimeMillis();
                if (currentMillis - lastDisplayMillis > DISPLAY_MILLIS) {
                    lastDisplayMillis = currentMillis;
                    logger.finer("thread count: core="
                            + pool.getCorePoolSize() + ", active="
                            + pool.getActiveCount() + ", tasks="
                            + taskCount);
                    if (null != lastEvent) {
                        logger.info("" + timer.getEventCount() + "/"
                                + taskCount + ", "
                                + timer.getProgressMessage(false) + ", "
                                + lastEvent[0].getDescription());

                        if (config.doPrintCurrRate()) {
                            String currMsg = timer.getCurrProgressMessage();
                            if (currMsg != null)
                                logger.info(currMsg);
                        }
                    }
                }

            } while (null != future);

            logger.finer("running = " + running
                         + ", terminated = " + pool.isTerminated()
                         + ", last future = " + lastFutureMillis);
            // currentMillis has already been set recently
            if (currentMillis - lastFutureMillis > futureMillis) {
                logger.warning("no futures received in over " + futureMillis + " ms");
                break;
            }
        } while (running && !pool.isTerminated());
        // NB - caller will set running to false to ensure exit
    }

    /**
     * @param logger
     */
    public void setLogger(SimpleLogger logger) {
        this.logger = logger;
    }

    /**
     * @param pool
     */
    public void setPool(ThreadPoolExecutor pool) {
        this.pool = pool;
    }

    /**
     *
     */
    public void incrementTaskCount() {
        // logger.info("incrementing " + taskCount);
        if (taskCountFinal) {
            // get the stack trace to track this down
            logger.logException("BUG!", new SyncException(
                    "increment to final task count"));
            return;
        }
        taskCount++;
    }

    public long getTaskCount() {
        return taskCount;
    }

    /**
     * @param count
     */
    public void setFinalTaskCount(long count) {
        synchronized (taskCountMutex) {
            if (taskCountFinal) {
                // get the stack trace to track this down
                throw new FatalException("BUG!", new SyncException(
                        "setter on final task count " + count));
            }
            if (count != taskCount) {
                // get the stack trace to track this down
                throw new FatalException("BUG!", new SyncException("setter on final task count " + count + " != " + taskCount));
            }
            logger.fine("setting " + count);
            taskCountFinal = true;
        }
    }

    /**
     *
     */
    public void checkThrottle() {
        // optional throttling
        if (!config.isThrottled()) {
            return;
        }

        long sleepMillis;
        double throttledEventsPerSecond = config
                .getThrottledEventsPerSecond();
        boolean isEvents = (throttledEventsPerSecond > 0);
        int throttledBytesPerSecond = isEvents ? 0 : config
                .getThrottledBytesPerSecond();
        logger.fine("throttling "
                + (isEvents
                // events
                ? (timer.getEventsPerSecond() + " tps to "
                        + throttledEventsPerSecond + " tps")
                        // bytes
                        : (timer.getBytesPerSecond() + " B/sec to "
                                + throttledBytesPerSecond + " B/sec")));
        // call the methods every time
        while ((throttledEventsPerSecond > 0 && (throttledEventsPerSecond < timer.getEventsPerSecond()))
                || (throttledBytesPerSecond > 0 && (throttledBytesPerSecond < timer
                        .getBytesPerSecond()))) {
            if (isEvents) {
                sleepMillis = (long) Math.ceil(Timer.MILLISECONDS_PER_SECOND
                                * ((timer.getEventCount() / throttledEventsPerSecond) - timer
                                        .getDurationSeconds()));
            } else {
                sleepMillis = (long) Math.ceil(Timer.MILLISECONDS_PER_SECOND
                                * ((timer.getBytes() / throttledBytesPerSecond) - timer
                                        .getDurationSeconds()));
            }
            sleepMillis = Math.max(sleepMillis, 1);
            logger.finer("sleeping " + sleepMillis);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // reset interrupt status and continue
                Thread.interrupted();
                logger.logException("interrupted", e);
            }
        }
        logger.fine("throttled to "
                + (isEvents ? (timer.getEventsPerSecond() + " tps")
                        : (timer.getBytesPerSecond() + " B/sec")));
    }

}
