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
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSyncManager {

    protected static SimpleLogger logger;

    /**
     *
     */
    private static final String ERROR_CODE_MISSING_URI_LEXICON = "XDMP-URILXCNNOTFOUND";

    private static final String ERROR_CODE_UNDEFINED_FUNCTION = "XDMP-UNDFUN";

    /**
     * @author Michael Blakeley, michael.blakeley@marklogic.com
     * 
     */
    public class CallerBlocksPolicy implements RejectedExecutionHandler {

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
                throw new RejectedExecutionException(e);
            }
        }

    }

    public static final String NAME = XQSyncManager.class.getName();

    private static final String START_VARIABLE_NAME = "start";

    private static final String START_POSITION_PREDICATE = "[position() ge $start]\n";

    private static final String START_POSITION_DEFINE_VARIABLE = "define variable $start as xs:integer external\n";

    private com.marklogic.ps.Session inputSession;

    private Configuration configuration;

    private long itemsQueued;

    private UriQueue uriQueue;

    private UriQueue lastUriQueue;

    /**
     * @param _config
     */
    public XQSyncManager(Configuration _config) {
        configuration = _config;
        logger = configuration.getLogger();
    }

    public void run() {
        Monitor monitor = null;

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
            workQueue = new ArrayBlockingQueue<Runnable>(queueSize);

            // CallerBlocksPolicy will automatically throttle the queue,
            // except for runs that use input-connection.
            RejectedExecutionHandler policy = new CallerBlocksPolicy();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(threads,
                    threads, 16, TimeUnit.SECONDS, workQueue, policy);
            CompletionService<TimedEvent[]> completionService = new ExecutorCompletionService<TimedEvent[]>(
                    pool);

            TaskFactory factory = new TaskFactory(configuration);

            // to attempt to avoid starvation, run the monitor with higher
            // priority than the thread pool will have.
            monitor = new Monitor(logger, pool, completionService,
                    configuration.isFatalErrors());
            monitor.setPriority(1 + Thread.NORM_PRIORITY);
            monitor.start();

            // to support large workloads we have an unbounded lightweight
            // queue, which feeds the completion service
            newUriQueue(completionService, pool, factory, monitor);

            Session outputSession = configuration.newOutputSession();
            if (null != outputSession) {
                ContentbaseMetaData meta = outputSession
                        .getContentbaseMetaData();
                logger.info("output version info: client "
                        + meta.getDriverVersionString() + ", server "
                        + meta.getServerVersionString());
            }

            if (null != inputSession) {
                ContentbaseMetaData meta = inputSession
                        .getContentbaseMetaData();
                logger.info("input version info: client "
                        + meta.getDriverVersionString() + ", server "
                        + meta.getServerVersionString());
                itemsQueued = queueFromInputConnection();
            } else {
                if (null != configuration.getInputPackagePath()) {
                    itemsQueued = queueFromInputPackage(configuration
                            .getInputPackagePath());
                } else {
                    itemsQueued = queueFromInputPath(configuration
                            .getInputPath());
                }
            }

            // no more tasks to queue - now we just wait
            monitor.setTaskCount(itemsQueued);
            logger.info("queued " + itemsQueued + " items");
            uriQueue.shutdown();

            logger.fine("queue size " + uriQueue.getQueueSize());
            while (uriQueue.getQueueSize() > 0) {
                Thread.sleep(125);
            }

            //monitor.setTaskCount(itemsQueued);

            /*
             * shut down the pool after queuing is complete and task count has
             * been set, not before then - to avoid races.
             */
            logger.fine("pool ready to shutdown");
            pool.shutdown();

            while (null != monitor && monitor.isAlive()) {
                try {
                    monitor.join();
                } catch (InterruptedException e) {
                    logger.logException("interrupted", e);
                }
            }

            factory.close();
            factory = null;

        } catch (Throwable t) {
            logger.logException("fatal error", t);
            // clean up
            if (null != uriQueue) {
                uriQueue.halt();
            }
            if (null != monitor) {
                monitor.halt(t);
            }
        }

        logger.fine("exiting");
    }

    /**
     * @param _completionService
     * @param _pool
     * @param _factory
     * @param _monitor
     */
    private void newUriQueue(
            CompletionService<TimedEvent[]> _completionService,
            ThreadPoolExecutor _pool, TaskFactory _factory,
            Monitor _monitor) {
        uriQueue = new UriQueue(configuration, _completionService, _pool,
                _factory, _monitor, new LinkedBlockingQueue<String>());
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
    private long queueFromInputPackage(String _path) throws IOException,
            SyncException {
        File file = new File(_path);

        if (file.isFile()) {
            return queueFromInputPackage(file);
        }

        if (!file.isDirectory()) {
            throw new IOException("unexpected file type: "
                    + file.getCanonicalPath());
        }

        // directory, so look for zip children
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
            total += queueFromInputPackage(childPath);
        }
        return total;
    }

    /**
     * @param _path
     * @return
     * @throws IOException
     * @throws SyncException
     */
    private long queueFromInputPackage(File _path) throws IOException,
            SyncException {
        // list contents of package
        logger.fine("listing package " + _path);

        // allow up to two active uriQueues
        while (null != lastUriQueue && 0 != lastUriQueue.getQueueSize()) {
            try {
                Thread.sleep(125);
            } catch (InterruptedException e) {
                logger.warning("interrupted, will continue");
            }
        }

        InputPackage inputPackage = new InputPackage(_path
                .getCanonicalPath(), configuration);
        // ensure that the package won't close while queuing
        inputPackage.addReference();
        logger.info("listing package " + _path + " ("
                + inputPackage.size() + ")");

        // create a new factory and queue for each input package
        // shutdown may be called multiple times - that is ok
        if (null != uriQueue) {
            uriQueue.shutdown();
        }
        lastUriQueue = uriQueue;
        newUriQueue(uriQueue, new PackageTaskFactory(configuration,
                inputPackage));
        logger
                .fine("uriQueue = " + uriQueue + ", last = "
                        + lastUriQueue);

        Iterator<String> iter = inputPackage.list().iterator();
        String path;
        long count = 0;

        while (iter.hasNext()) {
            path = iter.next();
            logger.fine("queuing " + count + ": " + path);
            inputPackage.addReference();
            uriQueue.add(path);
            count++;
        }

        // clean up so that the package can be closed
        uriQueue.shutdown();
        inputPackage.closeReference();
        logger.info("queued " + count + " from " + _path);
        return count;
    }

    /**
     * @param _old
     * @param _factory
     */
    private void newUriQueue(UriQueue _old, TaskFactory _factory) {
        // copy from old to new
        newUriQueue(_old.getCompletionService(), _old.getPool(),
                _factory, _old.getMonitor());
    }

    /**
     * @throws XccException
     * @throws SyncException
     */
    private long queueFromInputConnection() throws XccException,
            SyncException {
        // use lexicon by default - this may throw an exception
        try {
            return queueFromInputConnection(true);
        } catch (XQueryException e) {
            // check to see if the exception was XDMP-URILXCNNOTFOUND
            // for 3.1, check for missing cts:uris() function
            String code = e.getCode();
            if (ERROR_CODE_MISSING_URI_LEXICON.equals(code)
                    || ERROR_CODE_UNDEFINED_FUNCTION.equals(code)) {
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
     * @param _useLexicon
     * @throws XccException
     * @throws SyncException
     */
    private long queueFromInputConnection(boolean _useLexicon)
            throws XccException, SyncException {
        String[] collectionUris = configuration.getInputCollectionUris();
        String[] directoryUris = configuration.getInputDirectoryUris();
        String[] documentUris = configuration.getInputDocumentUris();
        String[] userQuery = configuration.getInputQuery();
        if (null != collectionUris && null != directoryUris) {
            logger.warning("conflicting properties: using "
                    + Configuration.INPUT_COLLECTION_URI_KEY + ", not "
                    + Configuration.INPUT_DIRECTORY_URI_KEY);
        }

        Long startPosition = configuration.getStartPosition();

        if (null != startPosition) {
            logger.info("using " + Configuration.INPUT_START_POSITION_KEY
                    + "=" + startPosition.longValue());
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
        logger.fine("buffer size = " + opts.getResultBufferSize()
                + ", caching = " + opts.getCacheResult());
        opts.setCacheResult(configuration.isInputQueryCachable());
        opts.setResultBufferSize(configuration.inputQueryBufferSize());
        logger.info("buffer size = " + opts.getResultBufferSize()
                + ", caching = " + opts.getCacheResult());

        String uri;
        ResultSequence rs;
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
                        : userQuery[i], startPosition, _useLexicon);
                request.setOptions(opts);

                rs = inputSession.submitRequest(request);

                while (rs.hasNext()) {
                    uri = rs.next().asString();
                    if (0 == count) {
                        logger.info("queuing first task: " + uri);
                    }
                    logger.fine("queuing " + count + ": " + uri);
                    uriQueue.add(uri);
                    count++;
                }
                rs.close();
            }
        } catch (StreamingResultException e) {
            logger.info("count = " + count);
            logger
                    .warning("Listing input URIs probably timed out:"
                            + " try setting "
                            + Configuration.INPUT_CACHABLE_KEY + " or "
                            + Configuration.INPUT_QUERY_BUFFER_BYTES_KEY);
            throw e;
        }
        return count;
    }

    /**
     * @param _collectionUri
     * @param _directoryUri
     * @param _userQuery
     * @param _startPosition
     * @param _useLexicon
     * @return
     * @throws XccException
     */
    private Request getRequest(String _collectionUri,
            String _directoryUri, String _userQuery, Long _startPosition,
            boolean _useLexicon) throws XccException {
        boolean hasStart = (_startPosition != null && _startPosition
                .longValue() > 1);
        Request request;
        // TODO allow limit by forest names? would only work with cts:uris()
        if (_collectionUri != null) {
            request = getCollectionRequest(_collectionUri, hasStart,
                    _useLexicon);

            // if requested, delete the collection
            if (configuration.isDeleteOutputCollection()) {
                Session outputSession = configuration.newOutputSession();
                if (outputSession != null) {
                    logger.info("deleting collection " + _collectionUri
                            + " on output connection");
                    outputSession.deleteCollection(_collectionUri);
                    outputSession.close();
                }
            }
        } else if (_directoryUri != null) {
            request = getDirectoryRequest(_directoryUri, hasStart,
                    _useLexicon);
        } else if (_userQuery != null) {
            // set list of uris via a user-supplied query
            logger.info("listing query: " + _userQuery);
            if (hasStart) {
                logger
                        .warning("ignoring start value in user-supplied query");
                hasStart = false;
            }
            request = inputSession.newAdhocQuery(_userQuery);
        } else {
            // list all the documents in the database
            request = getUrisRequest(hasStart, _useLexicon);
        }

        if (hasStart) {
            request.setNewIntegerVariable(START_VARIABLE_NAME,
                    _startPosition);
        }
        return request;
    }

    /**
     * @param _hasStart
     * @return
     */
    private Request getUrisRequest(boolean _hasStart, boolean _useLexicon) {
        String query = Session.XQUERY_VERSION_0_9_ML
                + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "");
        if (_useLexicon) {
            logger.info("listing all documents (with uri lexicon)");
            query += "cts:uris('', 'document')"
                    + (_hasStart ? START_POSITION_PREDICATE : "");
        } else {
            logger.info("listing all documents (no uri lexicon)");
            query += "for $i in doc()"
                    + (_hasStart ? START_POSITION_PREDICATE : "")
                    + " return string(xdmp:node-uri($i))";
        }
        logger.fine(query);
        return inputSession.newAdhocQuery(query);
    }

    /**
     * @param _uri
     * @param _hasStart
     */
    private Request getCollectionRequest(String _uri, boolean _hasStart,
            boolean _useLexicon) {
        logger.info("listing collection " + _uri);
        String query = Session.XQUERY_VERSION_0_9_ML
                + "define variable $uri as xs:string external\n"
                + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "");
        if (_useLexicon) {
            query += "cts:uris('', 'document', cts:collection-query($uri))\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "");
        } else {
            query += "for $i in collection($uri)\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(xdmp:node-uri($i))\n";
        }
        Request request = inputSession.newAdhocQuery(query);
        request.setNewStringVariable("uri", _uri);
        return request;
    }

    /**
     * @param _uri
     * @param _hasStart
     * @return
     */
    private Request getDirectoryRequest(String _uri, boolean _hasStart,
            boolean _useLexicon) {
        logger.info("listing directory " + _uri);
        String query = Session.XQUERY_VERSION_0_9_ML
                + "define variable $uri as xs:string external\n"
                + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "");
        if (_useLexicon) {
            query += "cts:uris('', 'document', cts:directory-query($uri, 'infinity'))\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "");
        } else {
            query += "for $i in xdmp:directory($uri, 'infinity')\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(xdmp:node-uri($i))\n";
        }
        logger.fine(query);
        Request request = inputSession.newAdhocQuery(query);
        String uri = _uri;
        if (!uri.endsWith("/")) {
            uri = uri + "/";
        }
        request.setNewStringVariable("uri", uri);
        return request;
    }

    /**
     * @param _inputPath
     * @return
     * @throws SyncException
     * @throws IOException
     */
    private long queueFromInputPath(String _inputPath)
            throws SyncException, IOException {
        // build documentList from a filesystem path
        // exclude stuff that ends with '.metadata'
        logger.info("listing from " + _inputPath + ", excluding "
                + XQSyncDocument.METADATA_REGEX);
        FileFinder ff = new FileFinder(_inputPath, null,
                XQSyncDocument.METADATA_REGEX);
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
