/*
 * Copyright (c)2004-2006 Mark Logic Corporation
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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.marklogic.ps.AbstractLoggableClass;
import com.marklogic.ps.FileFinder;
import com.marklogic.ps.Session;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSyncManager extends AbstractLoggableClass {

    /**
     * @author Michael Blakeley, michael.blakeley@marklogic.com
     * 
     */
    public class CallerBlocksPolicy implements RejectedExecutionHandler {

        private BlockingQueue<Runnable> queue;

        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable,
         *      java.util.concurrent.ThreadPoolExecutor)
         */
        public void rejectedExecution(Runnable r,
                ThreadPoolExecutor executor) {
            if (null == queue) {
                queue = executor.getQueue();
            }
            while (true) {
                try {
                    // block until space becomes available
                    queue.put(r);
                } catch (InterruptedException e) {
                    // someone is trying to interrupt us
                    throw new RejectedExecutionException(e);
                }
            }
        }

    }

    public static final String NAME = XQSyncManager.class.getName();

    private static final String START_VARIABLE_NAME = "start";

    private static final String START_POSITION_PREDICATE = "[position() ge $start]\n";

    private static final String START_POSITION_DEFINE_VARIABLE = "define variable $start as xs:integer external\n";

    private com.marklogic.ps.Session inputSession;

    private TaskFactory factory;

    private Configuration configuration;

    private long itemsQueued;

    /**
     * @param _config
     */
    public XQSyncManager(Configuration _config) {
        configuration = _config;
        logger = configuration.getLogger();
        XQSyncDocument.setLogger(logger);
    }

    public void run() {
        Monitor monitor = null;

        try {
            // start your engines...
            int threads = configuration.getThreadCount();
            logger.info("starting pool of " + threads + " threads");
            BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(
                    threads * 100 * 1000);
            // by using CallerBlocksPolicy, we automatically throttle the queue
            RejectedExecutionHandler policy = new CallerBlocksPolicy();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(threads,
                    threads, 16, TimeUnit.SECONDS, workQueue, policy);
            CompletionService completionService = new ExecutorCompletionService(
                    pool);

            // to attempt to avoid starvation, run the monitor with higher
            // priority than the thread pool will have.
            monitor = new Monitor(logger, pool, completionService);
            monitor.setPriority(Thread.NORM_PRIORITY + 1);
            monitor.start();

            // TODO move into a task-producer thread
            // eventually, this means no dedicated manager at all
            factory = new TaskFactory(configuration);

            inputSession = configuration.newInputSession();
            if (inputSession != null) {
                itemsQueued = queueFromInputConnection(completionService);
            } else if (configuration.getInputPackagePath() != null) {
                itemsQueued = queueFromInputPackage(completionService,
                        configuration.getInputPackagePath());
            } else {
                itemsQueued = queueFromInputPath(completionService,
                        configuration.getInputPath());
            }

            // no more tasks to queue: now we just wait
            if (inputSession != null) {
                inputSession.close();
            }
            logger.info("queued " + itemsQueued + " items");
            pool.shutdown();

            while (null != monitor && monitor.isAlive()) {
                try {
                    monitor.join();
                } catch (InterruptedException e) {
                    logger.logException("interrupted", e);
                }
            }

            factory.close();

        } catch (Throwable t) {
            if (monitor != null) {
                monitor.halt(t);
            }
        }

        logger.info("exiting");
    }

    /**
     * @param completionService
     * @return
     * @throws IOException
     * 
     */
    private long queueFromInputPackage(
            CompletionService completionService, String _path)
            throws IOException {
        File file = new File(_path);

        if (file.isFile()) {
            return queueFromInputPackage(completionService, file);
        }

        if (!file.isDirectory()) {
            throw new IOException("unexpected file type: "
                    + file.getCanonicalPath());
        }

        long total = 0;
        final String extension = Configuration.getPackageFileExtension();
        FileFilter filter = new FileFilter() {
            public boolean accept(File pathname) {
                return (pathname.isFile() && pathname.getName().endsWith(
                        extension));
            }
        };

        File[] children = file.listFiles(filter);
        Arrays.sort(children);

        String childPath;
        for (int i = 0; i < children.length; i++) {
            childPath = children[i].getCanonicalPath();
            total += queueFromInputPackage(completionService, childPath);
        }
        return total;
    }

    /**
     * @param completionService
     * @return
     * @throws IOException
     * 
     */
    @SuppressWarnings("unchecked")
    private long queueFromInputPackage(
            CompletionService completionService, File _path)
            throws IOException {
        // list contents of package
        logger.info("listing package " + _path);

        InputPackage inputPackage = new InputPackage(_path
                .getCanonicalPath());
        factory.setInputPackage(inputPackage);

        Iterator<String> iter = inputPackage.list().iterator();
        String path;
        long count = 0;

        while (iter.hasNext()) {
            count++;
            path = iter.next();
            logger.finer("queuing " + count + ": " + path);
            completionService.submit(factory.newCallableSync(path));
        }

        return count;
    }

    /**
     * @param completionService
     * @throws XccException
     */
    @SuppressWarnings("unchecked")
    private long queueFromInputConnection(
            CompletionService completionService) throws XccException {
        String[] collectionUris = configuration.getInputCollectionUris();
        String[] directoryUris = configuration.getInputDirectoryUris();
        String[] documentUris = configuration.getInputDocumentUris();
        String userQuery = configuration.getInputQuery();
        if (collectionUris != null && directoryUris != null) {
            logger.warning("conflicting properties: using "
                    + Configuration.INPUT_COLLECTION_URI_KEY + ", not "
                    + Configuration.INPUT_DIRECTORY_URI);
        }

        Long startPosition = configuration.getStartPosition();

        if (startPosition != null) {
            logger.info("using " + Configuration.INPUT_START_POSITION_KEY
                    + "=" + startPosition.longValue());
        }

        long count = 0;

        if (documentUris != null) {
            // we don't need to touch the database
            for (int i = 0; i < documentUris.length; i++) {
                count++;
                completionService.submit(factory
                        .newCallableSync(documentUris[i]));
            }
            return count;
        }

        // use multiple collections or dirs (but not both)
        int size = 1;
        if (collectionUris != null && collectionUris.length > size) {
            size = collectionUris.length;
        } else if (directoryUris != null && directoryUris.length > size) {
            size = directoryUris.length;
        }

        for (int i = 0; i < size; i++) {
            Request request = getRequest(collectionUris == null ? null
                    : collectionUris[i], directoryUris == null ? null
                    : directoryUris[i], userQuery, startPosition);
            RequestOptions opts = request.getEffectiveOptions();

            // in order to handle really big result sequences,
            // we have to turn off caching, and
            // we actually have to reduce the buffer size.
            logger.fine("buffer size = " + opts.getResultBufferSize());
            logger.fine("caching = " + opts.getCacheResult());
            opts.setCacheResult(false);
            opts.setResultBufferSize(4 * 1024);
            request.setOptions(opts);
            logger.fine("buffer size = " + opts.getResultBufferSize());
            logger.fine("caching = " + opts.getCacheResult());

            ResultSequence rs = inputSession.submitRequest(request);

            String uri;

            while (rs.hasNext()) {
                uri = rs.next().asString();
                logger.fine("queuing " + count + ": " + uri);
                completionService.submit(factory.newCallableSync(uri));
                count++;
            }
            rs.close();
        }

        return count;
    }

    /**
     * @param collectionUri
     * @param directoryUri
     * @param userQuery
     * @param startPosition
     * @return
     * @throws XccException
     */
    private Request getRequest(String collectionUri, String directoryUri,
            String userQuery, Long startPosition) throws XccException {
        boolean hasStart = (startPosition != null && startPosition
                .longValue() > 1);
        Request request;
        if (collectionUri != null) {
            request = getCollectionRequest(collectionUri, hasStart);

            // if requested, delete the collection
            if (configuration.isDeleteOutputCollection()) {
                Session outputSession = configuration.newOutputSession();
                if (outputSession != null) {
                    logger.info("deleting collection " + collectionUri
                            + " on output connection");
                    outputSession.deleteCollection(collectionUri);
                    outputSession.close();
                }
            }
        } else if (directoryUri != null) {
            request = getDirectoryRequest(directoryUri, hasStart);
        } else if (userQuery != null) {
            // set list of uris via a user-supplied query
            logger.info("listing query: " + userQuery);
            if (hasStart) {
                logger
                        .warning("ignoring start value in user-supplied query");
                hasStart = false;
            }
            request = inputSession.newAdhocQuery(userQuery);
        } else {
            // list all the documents in the database
            logger.info("listing all documents");
            String query = (hasStart ? START_POSITION_DEFINE_VARIABLE
                    : "")
                    + "for $i in doc()\n"
                    + (hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(base-uri($i))";
            request = inputSession.newAdhocQuery(query);
        }

        if (hasStart) {
            request.setNewIntegerVariable(START_VARIABLE_NAME,
                    startPosition);
        }
        return request;
    }

    /**
     * @param _uri
     * @param _hasStart
     */
    private Request getCollectionRequest(String _uri, boolean _hasStart) {
        logger.info("listing collection " + _uri);
        String query = "define variable $uri as xs:string external\n"
                + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in collection($uri)\n"
                + (_hasStart ? START_POSITION_PREDICATE : "")
                + "return string(base-uri($i))\n";
        Request request = inputSession.newAdhocQuery(query);
        request.setNewStringVariable("uri", _uri);
        return request;
    }

    /**
     * @param _uri
     * @param _hasStart
     * @return
     */
    private Request getDirectoryRequest(String _uri, boolean _hasStart) {
        logger.info("listing directory " + _uri);
        String query = "define variable $uri as xs:string external\n"
                + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in xdmp:directory($uri, 'infinity')\n"
                + (_hasStart ? START_POSITION_PREDICATE : "")
                + "return string(base-uri($i))\n";
        Request request = inputSession.newAdhocQuery(query);
        String uri = _uri;
        if (!uri.endsWith("/")) {
            uri = uri + "/";
        }
        request.setNewStringVariable("uri", uri);
        return request;
    }

    /**
     * @param _cs
     * @param _inputPath
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private long queueFromInputPath(CompletionService _cs,
            String _inputPath) throws IOException {
        // build documentList from a filesystem path
        // exclude stuff that ends with '.metadata'
        logger.info("listing from " + _inputPath + ", excluding "
                + XQSyncDocument.METADATA_REGEX);
        FileFinder ff = new FileFinder(_inputPath, null,
                XQSyncDocument.METADATA_REGEX);
        ff.find();

        Iterator<File> iter = ff.list().iterator();
        File file;
        long count = 0;
        while (iter.hasNext()) {
            count++;
            file = iter.next();
            logger.finer("queuing " + count + ": "
                    + file.getCanonicalPath());
            _cs.submit(factory.newCallableSync(file));
        }

        return count;
    }

    /**
     * @return
     */
    public long getItemsQueued() {
        return itemsQueued;
    }

}
