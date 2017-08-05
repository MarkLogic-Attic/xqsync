/**
 * Copyright (c) 2006-2012 MarkLogic Corporation. All rights reserved.
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
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class FragmentZipFiles {
    private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

    class FragmentTask implements Runnable {

        /**
         * 
         */
        private static final String ZIP_EXTENSION = ".zip";

        File file;

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

                // the problem is actually an overflow problem,
                // so we can't realistically check for it.

                fragment(path);
            } catch (Throwable t) {
                logger.logException("fatal error", t);
            }
        }

        private void fragment(String path) throws FileNotFoundException,
                IOException {
            logger.info("fragmenting path" + path);
            File parent = file.getParentFile();
            String basename = path.substring(0, path.length()
                    - ZIP_EXTENSION.length());
            String delimiter = "-";
            int fileIndex = 0;

            // fragment the archive
            ZipInputStream zis = new ZipInputStream(new FileInputStream(
                    file));
            ZipEntry srcEntry, dstEntry;
            ZipOutputStream output = null;
            long entries = 0;
            long size, compressedSize;
            String lastEntryName = null;
            String thisEntryName;
            long bytes;

            // iterate over all available entries,
            // copying to the current archive file.
            logger.fine("looking for entries in " + path);
            while ((srcEntry = zis.getNextEntry()) != null) {
                thisEntryName = srcEntry.getName();
                logger.finer("looking at entry " + thisEntryName + " in "
                        + path);
                entries++;

                logger.finer("output " + output + ", entries=" + entries);
                if (null == output
                        || entries >= OutputPackage.MAX_ENTRIES) {
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
                logger.finest("remembering entry " + thisEntryName);
                lastEntryName = thisEntryName;

                // duplicate the entry
                size = srcEntry.getSize();
                compressedSize = srcEntry.getCompressedSize();
                logger.finer("duplicating entry " + thisEntryName
                        + " in " + path + ": " + size + "; "
                        + compressedSize);
                // we must force recalc of the compressed size
                dstEntry = new ZipEntry(srcEntry);
                dstEntry.setCompressedSize(-1);
                output.putNextEntry(dstEntry);
                if (!srcEntry.isDirectory()) {
                    bytes = Utilities.copy(zis, output);
                    logger.finer("copied " + thisEntryName + ": " + bytes
                            + " Bytes");
                }
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
            logger.info("fragmented " + path);
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
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
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
