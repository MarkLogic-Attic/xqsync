/*
 * Copyright (c)2004-2007 Mark Logic Corporation
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

import com.marklogic.ps.AbstractLoggableClass;
import com.marklogic.ps.FileFinder;
import com.marklogic.ps.Session;
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
public class XQSyncManager extends AbstractLoggableClass {

    /**
     * 
     */
    private static final String ERROR_CODE_MISSING_URI_LEXICON = "XDMP-URILXCNNOTFOUND";

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
         * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable,
         *      java.util.concurrent.ThreadPoolExecutor)
         */
        public void rejectedExecution(Runnable r,
                ThreadPoolExecutor executor) {
            if (null == queue) {
                queue = executor.getQueue();
            }
            try {
                // block until space becomes available
                if (!warning) {
                    logger.warning("queue is full: size = "
                            + queue.size()
                            + " (warning will only appear once!)");
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
            BlockingQueue<Runnable> workQueue = null;
            inputSession = configuration.newInputSession();
            if (null != inputSession) {
                // we can't afford to block the input connection queue,
                // or else the XCC request might time out
                logger.info("starting pool of " + threads
                        + " threads, queue size = unlimited");
                workQueue = new LinkedBlockingQueue<Runnable>();
            } else {
                int queueSize = configuration.getQueueSize();
                logger.info("starting pool of " + threads
                        + " threads, queue size = " + queueSize);
                // an array queue should be somewhat lighter-weight
                workQueue = new ArrayBlockingQueue<Runnable>(queueSize);
            }
            // by using CallerBlocksPolicy, we automatically throttle the queue,
            // but this won't affect runs that use input-connection.
            RejectedExecutionHandler policy = new CallerBlocksPolicy();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(threads,
                    threads, 16, TimeUnit.SECONDS, workQueue, policy);
            CompletionService<TimedEvent> completionService = new ExecutorCompletionService<TimedEvent>(
                    pool);

            // to attempt to avoid starvation, run the monitor with higher
            // priority than the thread pool will have.
            monitor = new Monitor(logger, pool, completionService,
                    configuration.isFatalErrors());
            monitor.setPriority(Thread.NORM_PRIORITY + 1);
            monitor.start();

            // TODO move into a task-producer thread?
            // eventually, this means no dedicated manager at all
            factory = new TaskFactory(configuration);
            CallableWrapper.setFactory(factory);

            if (inputSession != null) {
                ContentbaseMetaData meta = inputSession
                        .getContentbaseMetaData();
                logger.info("version info: client "
                        + meta.getDriverVersionString() + ", server "
                        + meta.getServerVersionString());
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
            factory = null;

        } catch (Throwable t) {
            if (monitor != null) {
                monitor.halt(t);
            }
        }

        logger.info("exiting");
    }

    /**
     * @param _cs
     * @return
     * @throws IOException
     * 
     */
    private long queueFromInputPackage(CompletionService<TimedEvent> _cs,
            String _path) throws IOException {
        File file = new File(_path);

        if (file.isFile()) {
            return queueFromInputPackage(_cs, file);
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
            total += queueFromInputPackage(_cs, childPath);
        }
        return total;
    }

    /**
     * @param _cs
     * @return
     * @throws IOException
     * 
     */
    private long queueFromInputPackage(CompletionService<TimedEvent> _cs,
            File _path) throws IOException {
        // list contents of package
        logger.info("listing package " + _path);

        InputPackage inputPackage = new InputPackage(_path
                .getCanonicalPath());

        Iterator<String> iter = inputPackage.list().iterator();
        String path;
        long count = 0;

        while (iter.hasNext()) {
            count++;
            path = iter.next();
            logger.finer("queuing " + count + ": " + path);
            _cs.submit(new CallableWrapper(inputPackage, path));
        }

        return count;
    }

    /**
     * @param _cs
     * @throws XccException
     */
    private long queueFromInputConnection(
            CompletionService<TimedEvent> _cs) throws XccException {
        // use lexicon by default - this may throw an exception
        try {
            return queueFromInputConnection(_cs, true);
        } catch (XQueryException e) {
            // check to see if the exception was XDMP-URILXCNNOTFOUND
            if (ERROR_CODE_MISSING_URI_LEXICON.equals(e.getCode())) {
                // try again, the hard way
                logger.warning("Enable the document uri lexicon on "
                        + inputSession.getContentBaseName()
                        + " to speed up synchronization.");

                return queueFromInputConnection(_cs, false);
            }
            logger.logException("error queuing from input connection", e);
            throw e;
        }
    }

    /**
     * @param _cs
     * @param _useLexicon
     * @throws XccException
     */
    private long queueFromInputConnection(
            CompletionService<TimedEvent> _cs, boolean _useLexicon)
            throws XccException {
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
                _cs.submit(new CallableWrapper(documentUris[i]));
            }
            return count;
        }

        // use multiple collections or dirs (but not both)
        // TODO should find a way to avoid multiple calls, for this case
        int size = 1;
        if (collectionUris != null && collectionUris.length > size) {
            size = collectionUris.length;
        } else if (directoryUris != null && directoryUris.length > size) {
            size = directoryUris.length;
        }

        // in order to handle really big result sequences,
        // we may have to turn off caching (default),
        // and we may also have to *reduce* the buffer size.
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

        // There is an inherent conflict here, for large URI sets.
        // We want a limited queue size, to limit memory consumption,
        // but we must retrieve quickly so that XCC doesn't time out.
        // The current solution is limited queue size for most runs,
        // but an unlimited queue site when input-connection is used.
        // The queue is as light-weight as possible,
        // by using CallableWrapper instead of CallableSync.

        try {
            for (int i = 0; i < size; i++) {

                request = getRequest(collectionUris == null ? null
                        : collectionUris[i], directoryUris == null ? null
                        : directoryUris[i], userQuery, startPosition,
                        _useLexicon);
                request.setOptions(opts);
                rs = inputSession.submitRequest(request);

                while (rs.hasNext()) {
                    uri = rs.next().asString();
                    logger.fine("queuing " + count + ": " + uri);
                    _cs.submit(new CallableWrapper(uri));
                    count++;
                }
                rs.close();
            }
        } catch (StreamingResultException e) {
            logger.info("count = " + count);
            logger.warning("Listing input URIs probably timed out:"
                    + " try setting " + Configuration.INPUT_CACHABLE_KEY
                    + " or " + Configuration.INPUT_BUFFER_BYTES_KEY
                    + " or " + Configuration.QUEUE_SIZE_KEY);
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
        String query;
        logger.info("listing all documents");
        if (_useLexicon) {
            query = (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                    + "cts:uris('', 'document')\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "");
        } else {
            query = (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                    + "for $i in doc()\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(xdmp:node-uri($i))";
        }
        return inputSession.newAdhocQuery(query);
    }

    /**
     * @param _uri
     * @param _hasStart
     */
    private Request getCollectionRequest(String _uri, boolean _hasStart,
            boolean _useLexicon) {
        logger.info("listing collection " + _uri);
        String query;
        if (_useLexicon) {
            query = (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                    + "cts:uris('', 'document', cts:collection-query($uri))\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "");
        } else {
            query = "define variable $uri as xs:string external\n"
                    + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                    + "for $i in collection($uri)\n"
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
        String query;
        if (_useLexicon) {
            query = "define variable $uri as xs:string external\n"
                    + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                    + "cts:uris('', 'document', cts:directory-query($uri, 'infinity'))\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "");
        } else {
            query = "define variable $uri as xs:string external\n"
                    + (_hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                    + "for $i in xdmp:directory($uri, 'infinity')\n"
                    + (_hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(xdmp:node-uri($i))\n";
        }
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
    private long queueFromInputPath(CompletionService<TimedEvent> _cs,
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
            _cs.submit(new CallableWrapper(file));
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
