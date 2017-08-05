/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2004-2012 Mark Logic Corporation
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

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class CallableSync implements Callable<TimedEvent[]> {

    protected String[] inputUris;

    protected TaskFactory taskFactory;

    protected Monitor monitor;

    /**
     * @param _taskFactory
     * @param _uris
     */
    public CallableSync(TaskFactory _taskFactory, String[] _uris) {
        taskFactory = _taskFactory;
        inputUris = _uris;
        monitor = taskFactory.getMonitor();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public TimedEvent[] call() throws Exception {
        initialize();

        // revisit - throttle before or after creating the timed event? 
        monitor.checkThrottle();

        TimedEvent te[] = new TimedEvent[inputUris.length];
        for (int i = 0; i < inputUris.length; i++) {
            if (null == inputUris[i]) {
                continue;
            }
            te[i] = new TimedEvent();
        }

        // lazy initialization, to reduce memory
        Configuration configuration = taskFactory.getConfiguration();
        SimpleLogger logger = configuration.getLogger();
        ReaderInterface reader = taskFactory.getReader();
        if (null == reader) {
            throw new FatalException("null reader");
        }
        WriterInterface writer = taskFactory.getWriter();
        if (null == writer) {
            throw new FatalException("null writer");
        }

        logger.fine("starting sync of " + inputUris.length + ": "
                + inputUris[0]);

        try {
            DocumentInterface document = new XQSyncDocument(inputUris,
                    reader, writer, configuration);
            int bytesWritten = document.sync();
            for (int i = 0; i < te.length; i++) {
                if (null == te[i]) {
                    continue;
                }
                // all bytes go to the first event
                te[i].stop(0 == i ? bytesWritten : 0);
                te[i].setDescription(document.getOutputUri(i));
            }
            return te;
        } catch (SyncException e) {
            // we want to know which URI was at fault
            for (int i = 0; i < inputUris.length; i++)
                logger.severe("sync failed for: " + inputUris[i]);
            if (reader instanceof PackageReader) {
                logger.warning("error in input package "
                               + ((PackageReader) reader).getPath());
            }
            throw e;
        } catch (Throwable t) {
            // we want to know which URI was at fault
            for (int i = 0; i < inputUris.length; i++)
                logger.severe("sync failed for: " + inputUris[i]);
            throw new FatalException(t);
        } finally {
            if (null != reader) {
                reader.close();
            }
            // avoid starving other threads
            Thread.yield();
        }
    }

    /**
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private void initialize() throws NoSuchMethodException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        if (null == inputUris) {
            throw new NullPointerException(
                    "missing required field: inputUri");
        }

        if (null == taskFactory) {
            throw new NullPointerException(
                    "missing required field: taskFactory");
        }
    }

}
