/**
 * Copyright (c) 2009 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class PackageValidator {

    static SimpleLogger logger;

    static Collection<String> pathsObserved;

    static Map<String, String> urisObserved;

    private static Configuration config;

    private static PackageReader reader;

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        logger = SimpleLogger.getSimpleLogger();
        validatePaths(args);
    }

    /**
     * @param args
     * @throws IOException
     * @throws SyncException
     */
    private static void validatePaths(String[] args) throws IOException,
            SyncException {
        pathsObserved = new HashSet<String>();
        urisObserved = new HashMap<String, String>();
        config = new Configuration();
        config.setLogger(logger);
        reader = new PackageReader(config);
        File file;
        long count = 0;
        for (int i = 0; i < args.length; i++) {
            file = new File(args[i]);
            if (!file.exists()) {
                logger
                        .warning("file does not exist, skipping "
                                + args[i]);
                continue;
            }
            if (!file.canRead()) {
                logger.warning("cannot read file, skipping " + args[i]);
                continue;
            }
            if (file.isDirectory()) {
                logger.warning("skipping directory " + args[i]);
                continue;
            }
            try {
                count += validate(file);
            } catch (Exception e) {
                logger.logException(args[i], e);
            }
        }
        logger.info("checked " + count + " uris");
    }

    /**
     * @param file
     * @throws IOException
     * @throws SyncException
     */
    private static int validate(File file) throws IOException,
            SyncException {
        String canonicalPath = file.getCanonicalPath();
        logger.fine(canonicalPath);
        if (pathsObserved.contains(canonicalPath)) {
            logger.warning("skipping duplicate package " + canonicalPath);
            return 0;
        }
        pathsObserved.add(canonicalPath);

        // open and validate package
        InputPackage pkg = new InputPackage(canonicalPath, config);
        reader.setPackage(pkg);
        Iterator<String> iter = pkg.list().iterator();
        String uri;
        XQSyncDocument doc;
        int count = 0;
        if (!iter.hasNext()) {
            logger.warning("no uris found in " + canonicalPath);
            return 0;
        }
        while (iter.hasNext()) {
            uri = iter.next();
            if (urisObserved.containsKey(uri)) {
                logger.warning("duplicate uri in " + canonicalPath
                        + " from " + urisObserved.get(uri) + ": " + uri);
            } else {
                urisObserved.put(uri, canonicalPath);
            }
            doc = new XQSyncDocument(new String[] { uri }, reader, null,
                    config);
            doc.read();
            count++;
        }
        logger.info(canonicalPath + ": " + count + " ok");
        return count;
    }

}
