/**
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
import java.util.concurrent.Callable;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class TaskFactory {

    protected SimpleLogger logger;
    protected final Configuration configuration;
    protected WriterInterface[] writers;
    protected String outputPackagePath;
    protected volatile int count = 0;
    protected Monitor monitor;

    /**
     * @param config
     * @param monitor
     * @throws SyncException
     */
    public TaskFactory(Configuration config, Monitor monitor)
            throws SyncException {
        configuration = config;
        if (null == monitor) {
            throw new NullPointerException("monitor may not be null!");
        }
        this.monitor = monitor;

        logger = configuration.getLogger();

        outputPackagePath = config.getOutputPackagePath();

        if (null != outputPackagePath) {
            try {
                // create enough package writers to minimize contention
                int threadCount = config.getThreadCount();
                int poolSize = Math.min(Runtime.getRuntime().availableProcessors(), threadCount);
                logger.info("creating " + poolSize + " writer(s)");
                writers = new WriterInterface[poolSize];
                String path;
                String canonicalPath = new File(outputPackagePath).getCanonicalPath();
                for (int i = 0; i < poolSize; i++) {
                    path = OutputPackage.newPackagePath(canonicalPath, i, 3);
                    logger.fine("new writer " + path);
                    writers[i] = new PackageWriter(configuration, new OutputPackage(new File(path), configuration));
                }
            } catch (IOException e) {
                throw new SyncException(e);
            }
        }

    }

    /**
     * @param uris
     * @return
     */
    public Callable<TimedEvent[]> newTask(String[] uris) {
        return new CallableSync(this, uris);
    }

    /**
     * 
     */
    public void close() {
        if (null != writers && null != writers[0] && writers[0] instanceof PackageWriter) {
            logger.info("closing " + writers.length + " output package(s)");
            for (WriterInterface writer : writers) {
                if (null == writer) {
                    continue;
                }
                ((PackageWriter) writer).close();
            }
        }
    }

    /**
     * @return
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * @return
     * @throws SyncException
     */
    public ReaderInterface getReader() throws SyncException {
        return configuration.newReader();
    }

    /**
     * @return
     * @throws SyncException
     */
    public WriterInterface getWriter() throws SyncException {
        WriterInterface writer = null;
        // TODO handle sync to file path
        if (null != outputPackagePath) {
            // simple balancer, to keep threads from contending for packages
            writer = writers[count % writers.length];
            count++;
        } else {
            writer = configuration.newWriter();
        }
        return writer;
    }

    public Monitor getMonitor() {
        return monitor;
    }

}
