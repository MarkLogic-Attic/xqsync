/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.marklogic.ps.FileFinder;
import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.xcc.ContentbaseMetaData;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.StreamingResultException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class XQSyncManager {

    protected static SimpleLogger logger;

    /**
     *
     */
    private static final String ERROR_CODE_MISSING_URI_LEXICON = "XDMP-URILXCNNOTFOUND";

    /**
     * @author Michael Blakeley, michael.blakeley@marklogic.com
     *
     */
    public static class CallerBlocksPolicy implements RejectedExecutionHandler {

        private BlockingQueue<Runnable> queue;

        private boolean warning = false;

        /*
         * (non-Javadoc)
         *
         * @see
         * java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java
         * .lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
         */
        public void rejectedExecution(Runnable r,
                ThreadPoolExecutor executor) {
            if (null == queue) {
                queue = executor.getQueue();
            }
            try {
                // block until space becomes available
                if (!warning) {
                    logger.fine("queue is full: size = " + queue.size()
                            + " (will only appear once!)");
                    warning = true;
                }
                queue.put(r);
            } catch (InterruptedException e) {
                // someone is trying to interrupt us
                // reset interrupt status and continue
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException(e);
            }
        }

    }

    public static final String NAME = XQSyncManager.class.getName();
    private static final String START_VARIABLE_NAME = "start";
    private static final String START_POSITION_PREDICATE = "[position() ge $start]\n";
    private static final String START_POSITION_DEFINE_VARIABLE = "declare variable $start as xs:integer external;\n";
    private com.marklogic.ps.Session inputSession;
    private final Configuration configuration;
    private long itemsQueued;
    private UriQueue uriQueue;
    private UriQueue lastUriQueue;
    private Monitor monitor;

    /**
     * @param config
     */
    public XQSyncManager(Configuration config) {
        configuration = config;
        logger = configuration.getLogger();
    }

    public void run() {
        TaskFactory factory = null;
        try {
            // start your engines...
            int threads = configuration.getThreadCount();
            logger.finer("threads = " + threads);
            BlockingQueue<Runnable> workQueue = null;
            inputSession = configuration.newInputSession();
            int queueSize = configuration.getQueueSize();
            logger.info("starting pool of " + threads
                    + " threads, queue size = " + queueSize);
            // an array queue should be somewhat lighter-weight
            workQueue = new ArrayBlockingQueue<>(queueSize);

            // CallerBlocksPolicy will automatically throttle the queue,
            // except for runs that use input-connection.
            RejectedExecutionHandler policy = new CallerBlocksPolicy();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(threads, threads, 16, TimeUnit.SECONDS, workQueue, policy);
            CompletionService<TimedEvent[]> completionService = new ExecutorCompletionService<>(pool);

            // to attempt to avoid starvation, run the monitor with higher
            // priority than the thread pool will have.
            monitor = new Monitor(configuration, pool, completionService, configuration.isFatalErrors());
            monitor.setPriority(1 + Thread.NORM_PRIORITY);
            monitor.start();

            factory = new TaskFactory(configuration, monitor);

            // to support large workloads we have an unbounded lightweight
            // queue, which feeds the completion service
            newUriQueue(completionService, pool, factory, monitor);

            try (Session outputSession = configuration.newOutputSession()) {
                if (null != outputSession) {
                    ContentbaseMetaData meta = outputSession.getContentbaseMetaData();
                    logger.info("output version info: client "
                            + meta.getDriverVersionString() + ", server "
                            + meta.getServerVersionString());
                }
            }

            if (null != inputSession) {
                ContentbaseMetaData meta = inputSession.getContentbaseMetaData();
                logger.info("input version info: client "
                        + meta.getDriverVersionString() + ", server "
                        + meta.getServerVersionString());
                itemsQueued = queueFromInputConnection();
            } else {
                if (null != configuration.getInputPackagePath()) {
                    itemsQueued = queueFromInputPackage(configuration.getInputPackagePath());
                } else {
                    itemsQueued = queueFromInputPath(configuration.getInputPath());
                }
            }

            // no more tasks to queue - now we just wait
            while (monitor.getTaskCount() != itemsQueued) {
                Thread.sleep(125);
                Thread.yield();
                if (monitor.getTaskCount() > itemsQueued) {
                    throw new FatalException("task count mismatch: "
                            + itemsQueued + " < "
                            + monitor.getTaskCount());
                }
            }
            monitor.setFinalTaskCount(itemsQueued);
            logger.info("final queue count " + itemsQueued);
            uriQueue.shutdown();

            logger.fine("queue is shutdown with queue size "
                    + uriQueue.getQueueSize());
            // loop until all uri tasks have been queued
            do {
                /* Give the queue a chance to start, before testing it.
                 * After that, the queue should fill much more quickly
                 * than the thread pool can drain it.
                 */
                Thread.sleep(125);
                Thread.yield();
            } while (uriQueue.getQueueSize() > 0);

            /*
             * shut down the pool after queuing is complete and task count has
             * been set, not before then - to avoid races.
             */
            logger.info("pool ready to shutdown, queue size " + uriQueue.getQueueSize());
            pool.shutdown();

            logger.info("waiting for monitor to exit");
            do {
                logger.finest("waiting for monitor " + monitor + " " + (null != monitor) + " " + monitor.isAlive());
                try {
                    Thread.yield();
                    monitor.join();
                } catch (InterruptedException e) {
                    // reset interrupt status and continue
                    Thread.interrupted();
                    logger.logException("interrupted", e);
                }
                logger.finest("waiting for monitor " + monitor + " " + (null != monitor) + " " + monitor.isAlive());
            } while (null != monitor && monitor.isAlive());
        } catch (Throwable t) {
            logger.logException("fatal error", t);
            // clean up
            // first, ensure no new new tasks are queued
            if (null != uriQueue) {
                uriQueue.halt();
            }
            // tell the monitor to stop running
            if (null != monitor) {
                logger.info("halting monitor");
                monitor.halt(t);
            }
        } finally {
            // important to do this one last close. If we are outputting
            // zip files, and this is not done, the last zip file might
            // be corrupted.
            if (null != factory) {
                logger.info("closing factory");
                factory.close();
                factory = null;
            }

            // tell the configuration to clean up anything that needs it
            if (null != configuration) {
                configuration.close();
            }
        }

        logger.fine("exiting");
    }

    /**
     * @param completionService
     * @param pool
     * @param factory
     * @param monitor
     */
    private void newUriQueue(CompletionService<TimedEvent[]> completionService, ThreadPoolExecutor pool, TaskFactory factory, Monitor monitor) {
        uriQueue = new UriQueue(configuration, completionService, pool, factory, monitor, new LinkedBlockingQueue<>());
        uriQueue.start();
        // do not proceed until the uriQueue is running - fixes race
        while (!uriQueue.isActive()) {
            Thread.yield();
        }
    }

    /**
     * @return
     * @throws IOException
     * @throws SyncException
     *
     */
    private long queueFromInputPackage(String path) throws IOException,
            SyncException {
        logger.fine(path);
        File file = new File(path);

        if (!file.exists()) {
            throw new IOException("missing expected input package path: " + path);
        }

        if (!file.canRead()) {
            throw new IOException("cannot read from input package path: " + path);
        }

        if (file.isFile()) {
            return queueFromInputPackageFile(file);
        }

        if (!file.isDirectory()) {
            throw new IOException("unexpected file type: " + file.getCanonicalPath());
        }

        // directory, so look for zip children
        long total = 0;
        final String extension = Configuration.getPackageFileExtension();
        FileFilter filter = pathname -> (pathname.isDirectory() || (pathname.isFile() && pathname.getName().endsWith(extension)));

        File[] children = file.listFiles(filter);
        Arrays.sort(children);

        String childPath;
        for (File child : children) {
            childPath = child.getCanonicalPath();
            total += queueFromInputPackage(childPath);
        }
        return total;
    }

    /**
     * @param path
     * @return
     * @throws IOException
     * @throws SyncException
     */
    private long queueFromInputPackageFile(File path) throws IOException, SyncException {
        // list contents of package
        logger.fine("listing package " + path);

        // allow up to two active uriQueues
        while (null != lastUriQueue && 0 != lastUriQueue.getQueueSize()) {
            try {
                Thread.sleep(125);
            } catch (InterruptedException e) {
                // reset interrupt status and continue
                Thread.currentThread().interrupt();
                logger.warning("interrupted, will continue");
            }
        }

        InputPackage inputPackage = new InputPackage(path.getCanonicalPath(), configuration);
        // ensure that the package won't close while euing
        inputPackage.addReference();
        logger.fine("listing package " + path + " (" + inputPackage.size() + ")");

        // create a new factory and queue for each input package
        // shutdown may be called multiple times - that is ok
        if (null != uriQueue) {
            uriQueue.shutdown();
        }
        lastUriQueue = uriQueue;
        newUriQueue(uriQueue, new PackageTaskFactory(configuration, monitor, inputPackage));
        logger.fine("uriQueue = " + uriQueue + ", last = " + lastUriQueue);

        Iterator<String> iter = inputPackage.list().iterator();
        String inputPackagePath;
        long count = 0;

        while (iter.hasNext()) {
            inputPackagePath = iter.next();
            logger.finest("queuing " + count + ": " + inputPackagePath);
            inputPackage.addReference();
            uriQueue.add(inputPackagePath);
            count++;
        }

        // clean up so that the package can be closed
        uriQueue.shutdown();
        inputPackage.closeReference();
        logger.info("queued " + count + " from " + path);
        return count;
    }

    /**
     * @param old
     * @param factory
     */
    private void newUriQueue(UriQueue old, TaskFactory factory) {
        // copy from old to new
        newUriQueue(old.getCompletionService(), old.getPool(), factory, old.getMonitor());
    }

    /**
     * @throws XccException
     * @throws SyncException
     */
    private long queueFromInputConnection() throws XccException {
        // use lexicon by default - this may throw an exception
        try {
            return queueFromInputConnection(true);
        } catch (XQueryException e) {
            // check to see if the exception was XDMP-URILXCNNOTFOUND
            String code = e.getCode();
            if (ERROR_CODE_MISSING_URI_LEXICON.equals(code)) {
                // try again, the hard way
                logger.warning("Enable the document uri lexicon on "
                        + inputSession.getContentBaseName()
                        + " to speed up synchronization.");

                return queueFromInputConnection(false);
            }
            logger.logException("error queuing from input connection", e);
            throw e;
        }
    }

    /**
     * @param useLexicon
     * @throws XccException
     * @throws SyncException
     */
    private long queueFromInputConnection(boolean useLexicon) throws XccException {
        String[] collectionUris = configuration.getInputCollectionUris();
        String[] directoryUris = configuration.getInputDirectoryUris();
        String[] documentUris = configuration.getInputDocumentUris();
        String[] userQuery = configuration.getInputQuery();
        // warn the user about incompatible combinations
        if (null != documentUris) {
            if (null != collectionUris
                || null != directoryUris
                || null != userQuery) {
                logger.warning("conflicting properties: only using " + Configuration.INPUT_DOCUMENT_URIS_KEY);
            }
        } else if (null != collectionUris) {
            if (null != directoryUris || null != userQuery) {
                logger.warning("conflicting properties: only using " + Configuration.INPUT_COLLECTION_URI_KEY);
            }
        } else if (null != directoryUris && null != userQuery) {
            logger.warning("conflicting properties: only using " + Configuration.INPUT_DIRECTORY_URI_KEY);
        }

        Long startPosition = configuration.getStartPosition();

        if (null != startPosition) {
            logger.info("using " + Configuration.INPUT_START_POSITION_KEY + "=" + startPosition);
        }

        long count = 0;

        if (null != documentUris) {
            // we don't need to touch the database to get the uris
            for (int i = 0; i < documentUris.length; i++) {
                if (null != startPosition && i < startPosition) {
                    continue;
                }
                uriQueue.add(documentUris[i]);
                count++;
            }
            return count;
        }

        // XCC has trouble caching really large result sequences
        // This will all end up in RAM anyway...
        RequestOptions opts = inputSession.getDefaultRequestOptions();
        logger.fine("buffer size = " + opts.getResultBufferSize() + ", caching = " + opts.getCacheResult());
        opts.setCacheResult(configuration.isInputQueryCachable());
        opts.setResultBufferSize(configuration.inputQueryBufferSize());
        logger.info("buffer size = " + opts.getResultBufferSize() + ", caching = " + opts.getCacheResult());

        String uri;
        Request request;

        // support multiple collections or directories (but not both)
        // TODO find a way to avoid 1 request per collection or directory?
        int size = 1;
        if (null != collectionUris && collectionUris.length > size) {
            size = collectionUris.length;
        } else if (null != directoryUris && directoryUris.length > size) {
            size = directoryUris.length;
        } else if (null != userQuery && userQuery.length > size) {
            size = userQuery.length;
        }

        /*
         * There is an inherent conflict here, for large URI sets. We want a
         * limited queue size, to limit memory consumption, but we must retrieve
         * quickly so that XCC doesn't time out. The current solution is to read
         * all the results at once, but use an extra thread to queue them as
         * CallableSync objects with a CallerBlocksPolicy.
         */
        try {
            for (int i = 0; i < size; i++) {
                request = getRequest(null == collectionUris ? null
                        : collectionUris[i], null == directoryUris ? null
                        : directoryUris[i], null == userQuery ? null
                        : userQuery[i], startPosition, useLexicon);
                request.setOptions(opts);

                try (ResultSequence rs = inputSession.submitRequest(request)){
                    while (rs.hasNext()) {
                        uri = rs.next().asString();
                        if (0 == count) {
                            logger.info("queuing first task: " + uri);
                        }
                        logger.finest("queuing " + count + ": " + uri);
                        uriQueue.add(uri);
                        count++;
                    }
                }
            }
        } catch (StreamingResultException e) {
            logger.info("count = " + count);
            logger.warning("Listing input URIs probably timed out:"
                            + " try setting "
                            + Configuration.INPUT_CACHABLE_KEY + " or "
                            + Configuration.INPUT_QUERY_BUFFER_BYTES_KEY);
            throw e;
        }
        return count;
    }

    /**
     * @param collectionUri
     * @param directoryUri
     * @param userQuery
     * @param startPosition
     * @param useLexicon
     * @return
     * @throws XccException
     */
    private Request getRequest(String collectionUri, String directoryUri, String userQuery, Long startPosition, boolean useLexicon) throws XccException {
        boolean hasStart = (startPosition != null && startPosition > 1);
        Request request;
        // TODO allow limit by forest names? would only work with cts:uris()
        if (collectionUri != null) {
            request = getCollectionRequest(collectionUri, hasStart, useLexicon);

            // if requested, delete the collection
            if (configuration.isDeleteOutputCollection()) {
                try (Session outputSession = configuration.newOutputSession()) {
                    if (outputSession != null) {
                        logger.info("deleting collection " + collectionUri + " on output connection");
                        outputSession.deleteCollection(collectionUri);
                    }
                }
            }
        } else if (directoryUri != null) {
            request = getDirectoryRequest(directoryUri, hasStart, useLexicon);
        } else if (userQuery != null) {
            // set list of uris via a user-supplied query
            logger.info("listing query: " + userQuery);
            if (hasStart) {
                logger.warning("ignoring start value in user-supplied query");
                hasStart = false;
            }
            request = inputSession.newAdhocQuery(userQuery);
        } else {
            // list all the documents in the database
            request = getUrisRequest(hasStart, useLexicon);
        }

        if (hasStart) {
            request.setNewIntegerVariable(START_VARIABLE_NAME, startPosition);
        }
        return request;
    }

    /**
     * @param hasStart
     * @param useLexicon
     * @return
     */
    private Request getUrisRequest(boolean hasStart, boolean useLexicon) {
        String query = Session.XQUERY_VERSION_1_0_ML + (hasStart ? START_POSITION_DEFINE_VARIABLE : "");
        if (useLexicon) {
            logger.info("listing all documents (with uri lexicon)");
            query += "cts:uris('', 'document')" + (hasStart ? START_POSITION_PREDICATE : "");
        } else {
            logger.info("listing all documents (no uri lexicon)");
            query += "for $i in doc()"
                    + (hasStart ? START_POSITION_PREDICATE : "")
                    + " return string(xdmp:node-uri($i))";
        }
        logger.fine(query);
        return inputSession.newAdhocQuery(query);
    }

    /**
     * @param uri
     * @param hasStart
     */
    private Request getCollectionRequest(String uri, boolean hasStart, boolean useLexicon) {
        logger.info("listing collection " + uri);
        String query = Session.XQUERY_VERSION_1_0_ML
                + "declare variable $uri as xs:string external;\n"
                + (hasStart ? START_POSITION_DEFINE_VARIABLE : "");
        if (useLexicon) {
            query += "cts:uris('', 'document', cts:collection-query($uri))\n" + (hasStart ? START_POSITION_PREDICATE : "");
        } else {
            query += "for $i in collection($uri)\n"
                    + (hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(xdmp:node-uri($i))\n";
        }
        Request request = inputSession.newAdhocQuery(query);
        request.setNewStringVariable("uri", uri);
        return request;
    }

    /**
     * @param uri
     * @param hasStart
     * @param useLexicon
     * @return
     */
    private Request getDirectoryRequest(String uri, boolean hasStart, boolean useLexicon) {
        logger.info("listing directory " + uri);
        String query = Session.XQUERY_VERSION_1_0_ML
                + "declare variable $uri as xs:string external;\n"
                + (hasStart ? START_POSITION_DEFINE_VARIABLE : "");
        if (useLexicon) {
            query += "cts:uris('', 'document', cts:directory-query($uri, 'infinity'))\n" + (hasStart ? START_POSITION_PREDICATE : "");
        } else {
            query += "for $i in xdmp:directory($uri, 'infinity')\n"
                    + (hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(xdmp:node-uri($i))\n";
        }
        logger.fine(query);
        Request request = inputSession.newAdhocQuery(query);
        String tempUri = uri;
        if (!uri.endsWith("/")) {
            tempUri = uri + "/";
        }
        request.setNewStringVariable("uri", tempUri);
        return request;
    }

    /**
     * @param inputPath
     * @return
     * @throws SyncException
     * @throws IOException
     */
    private long queueFromInputPath(String inputPath) throws IOException {
        // build documentList from a filesystem path
        // exclude stuff that ends with '.metadata'
        logger.info("listing from " + inputPath + ", excluding " + XQSyncDocument.METADATA_REGEX);
        FileFinder ff = new FileFinder(inputPath, null, XQSyncDocument.METADATA_REGEX);
        ff.find();

        Iterator<File> iter = ff.list().iterator();
        File file;
        String canonicalPath;
        long count = 0;
        while (iter.hasNext()) {
            count++;
            file = iter.next();
            canonicalPath = file.getCanonicalPath();
            logger.finer("queuing " + count + ": " + canonicalPath);
            uriQueue.add(canonicalPath);
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
