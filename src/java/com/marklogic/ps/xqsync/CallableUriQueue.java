/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class CallableUriQueue implements Callable<TimedEvent> {

    protected static final long SLEEP_MILLIS = 125;

    protected static final String POISON = "";

    protected Configuration configuration;

    protected volatile BlockingQueue<String> queue;

    protected TaskFactory factory;

    protected CompletionService<TimedEvent> completionService;

    protected boolean active;

    /**
     * @param _configuration
     * @param _cs
     * @param _factory
     * @param _queue
     */
    public CallableUriQueue(Configuration _configuration,
            CompletionService<TimedEvent> _cs, TaskFactory _factory,
            BlockingQueue<String> _queue) {
        configuration = _configuration;
        factory = _factory;
        queue = _queue;
        completionService = _cs;
        active = true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public TimedEvent call() throws Exception {
        SimpleLogger logger = configuration.getLogger();
        long count = 0;

        while (active) {
            String uri = null;
            // block until something is available
            try {
                uri = queue.take();
                if (0 == count) {
                    logger.fine("took first uri: " + uri);
                }
            } catch (InterruptedException e) {
                logger.warning("take interrupted: continuing after "
                        + e.getMessage());
                continue;
            }
            //logger.fine("uri = " + uri);
            if (null == uri) {
                throw new NullPointerException("null uri");
            }
            if (POISON.equals(uri)) {
                logger.fine("uri = POISON");
                active = false;
                break;
            }
            completionService.submit(factory.newTask(uri));
            count++;
        }
        logger.fine("finished queuing " + count + " uris");
        return null;
    }

}
