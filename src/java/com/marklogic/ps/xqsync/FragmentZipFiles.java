/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class FragmentZipFiles {
    // number of entries overflows at 2^16 = 65536
    // ref: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4828461
    // (supposed to be fixed, but isn't)
    private static final int MAX_ENTRIES = 65536 - 1;

    // we use a lower limit, to allow for extra metadata crossovers
    private static final int MAX_ENTRIES_SOFT = MAX_ENTRIES - 3;

    private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

    class FragmentTask implements Runnable {

        /**
         * 
         */
        private static final String ZIP_EXTENSION = ".zip";

        File file;

        private FragmentTask() {
            super();
        }

        public FragmentTask(File _file) {
            file = _file;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        public void run() {
            try {
                String path = file.getCanonicalPath();
                if (!file.getName().endsWith(ZIP_EXTENSION)) {
                    logger.info("skipping non-zip file " + path);
                    return;
                }

                ZipFile zf = new ZipFile(file);
                int size = zf.size();

                // the problem is actually an overflow problem,
                // so we can't realistically check for it.
                // if (size < MAX_ENTRIES_REAL) {
                // logger.info("skipping already-fragmented file "
                // + path + " (" + size + " entries)");
                // return;
                // }
                zf.close();
                logger.info("fragmenting path" + path
                        + ": claims to have " + size + " entries");

                fragment(path);
                logger.info("fragmented " + path);
            } catch (Throwable t) {
                logger.logException("fatal error", t);
            }
        }

        private void fragment(String path) throws FileNotFoundException,
                IOException {
            File parent = file.getParentFile();
            String basename = path.substring(0, path.length()
                    - ZIP_EXTENSION.length());
            String delimiter = "-";
            int fileIndex = 0;

            // fragment the archive
            ZipInputStream zis = new ZipInputStream(new FileInputStream(
                    file));
            ZipEntry entry;
            ZipOutputStream output = null;
            long entries = 0;
            String lastEntryName = null;
            String thisEntryName;

            // iterate over all available entries,
            // copying to the current archive file.
            logger.fine("looking for entries in " + path);
            while ((entry = zis.getNextEntry()) != null) {
                thisEntryName = entry.getName();
                logger.finer("looking at entry " + thisEntryName + " in "
                        + path);
                entries++;

                logger.finer("output " + output + ", entries=" + entries);
                if (null == output || entries >= MAX_ENTRIES_SOFT) {
                    logger.fine("new output needed");
                    // ensure that we keep metadata and content together
                    if (areRelated(lastEntryName, thisEntryName)) {
                        logger
                                .info("keeping content and metadata together for "
                                        + lastEntryName);
                        // we can go for one more entry, luckily
                    } else {
                        logger.fine("new output will be created");
                        if (null != output) {
                            output.flush();
                            output.close();
                        }
                        entries = 0;
                        output = nextOutput(basename, delimiter,
                                fileIndex++, parent);
                    }
                }

                // this helps us keep metadata with its content
                logger.finest("remembering entry" + thisEntryName);
                lastEntryName = thisEntryName;

                // duplicate the entry
                logger.finer("duplicating entry " + thisEntryName
                        + " in " + path);
                output.putNextEntry(entry);
                Utilities.copy(zis, output);
                output.closeEntry();
                output.flush();

                logger.fine("processed entry " + entries + ": "
                        + lastEntryName);
            }

            if (null != output) {
                output.flush();
                output.close();
            }

            zis.close();
        }

        /**
         * @param lastName
         * @param thisName
         * @return
         */
        private boolean areRelated(String lastName, String thisName) {
            // is one of these the content for the other?
            // logger.fine("a=" + lastName + ", b=" + thisName);
            return lastName != null
                    && thisName != null
                    && (lastName.endsWith(XQSyncDocument.METADATA_EXT) || thisName
                            .endsWith(XQSyncDocument.METADATA_EXT))
                    && (lastName.startsWith(thisName) || thisName
                            .startsWith(lastName));
        }

        /**
         * @param basename
         * @param delimiter
         * @param fileIndex
         * @param parent
         * @return
         * @throws IOException
         */
        private ZipOutputStream nextOutput(String basename,
                String delimiter, int fileIndex, File parent)
                throws IOException {
            String nextName = nextName(basename, delimiter, fileIndex++);
            File outFile = new File(parent, nextName);
            logger.info("opening new zip file: "
                    + outFile.getCanonicalPath());
            ZipOutputStream output = new ZipOutputStream(
                    new FileOutputStream(outFile));
            return output;
        }

        /**
         * @param basename
         * @param i
         * @return
         */
        private String nextName(String basename, String delimiter, int i) {
            return basename + delimiter
                    + String.format("%04d", new Integer(i))
                    + ZIP_EXTENSION;
        }

    };

    /**
     * @param args
     * @throws UTFDataFormatException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws UTFDataFormatException,
            InterruptedException {
        String encoding = System.getProperty("file.encoding");
        if (!"UTF-8".equals(encoding)) {
            throw new UTFDataFormatException("system encoding "
                    + encoding + "is not UTF-8");
        }

        logger.configureLogger(System.getProperties());

        int threads = Integer.parseInt(System.getProperty("THREADS", ""
                + Runtime.getRuntime().availableProcessors()));
        int capacity = 1000 * threads;

        // given input zip files, start a thread to fragment each one
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(
                capacity);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                threads, threads, 60, TimeUnit.SECONDS, workQueue);
        threadPoolExecutor.prestartAllCoreThreads();

        File file;
        FragmentZipFiles factory = new FragmentZipFiles();
        for (int i = 0; i < args.length; i++) {
            file = new File(args[i]);
            FragmentTask task = factory.new FragmentTask(file);
            threadPoolExecutor.submit(task);
        }

        threadPoolExecutor.shutdown();

        while (!threadPoolExecutor.isTerminated()) {
            threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        logger.info("all files completed");
    }
}
