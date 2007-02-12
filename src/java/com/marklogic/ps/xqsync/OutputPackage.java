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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.marklogic.ps.AbstractLoggableClass;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class OutputPackage extends AbstractLoggableClass {

    // number of entries overflows at 2^16 = 65536
    // ref: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4828461
    // (supposed to be fixed, but isn't)
    private static final int MAX_ENTRIES = 65536 - 1;

    public static String EXTENSION = ".zip";

    private long currentFileBytes = 0;

    private Object outputMutex = new Object();

    private ZipOutputStream outputStream;

    private File constructorFile;

    private File currentFile;

    private int fileCount = 0;

    private int currentEntries;

    /**
     * @param _file
     */
    public OutputPackage(File _file) {
        constructorFile = _file;
    }

    /**
     * @throws IOException
     * 
     */
    public void close() throws IOException {
        synchronized (outputMutex) {
            if (outputStream == null) {
                return;
            }
            outputStream.close();
        }
    }

    /**
     * @throws IOException
     * 
     */
    public void flush() throws IOException {
        synchronized (outputMutex) {
            if (outputStream == null) {
                return;
            }
            outputStream.flush();
        }
    }

    /**
     * @param outputPath
     * @param bytes
     * @param metadata
     * @throws IOException
     */
    public void write(String outputPath, byte[] bytes,
            XQSyncDocumentMetadata metadata) throws IOException {
        /*
         * This method uses size metrics to automatically manage multiple zip
         * archives, to avoid 32-bit limits in java.util.zip
         * 
         * An exception-based mechanism would be tricky, here: we definitely
         * want the content and the meta entries to stay in the same zipfile.
         */
        byte[] metaBytes = metadata.toXML().getBytes();
        long total = bytes.length + metaBytes.length;
        ZipEntry entry = new ZipEntry(outputPath);

        String metadataPath = XQSyncDocument.getMetadataPath(outputPath);
        ZipEntry metaEntry = new ZipEntry(metadataPath);

        // TODO change to java.concurrent reentrantlock?
        synchronized (outputMutex) {
            if (outputStream == null) {
                // lazily construct a new zipfile outputstream
                logger.fine("no existing package");
                newOutputStream();
            }

            // by checking outputBytes first, we should avoid infinite loops -
            // at the cost of fatal exceptions.
            if (currentFileBytes > 0
                    && currentFileBytes + total > Integer.MAX_VALUE) {
                logger.fine("too many bytes in current package");
                newOutputStream();
            }

            // don't create zips that Java can't read back in
            if (currentEntries > 0 && (currentEntries + 2) >= MAX_ENTRIES) {
                logger.fine("too many entries in current package");
                newOutputStream();
            }

            outputStream.putNextEntry(entry);
            outputStream.write(bytes);
            outputStream.closeEntry();

            outputStream.putNextEntry(metaEntry);
            outputStream.write(metaBytes);
            outputStream.closeEntry();
        }
        currentFileBytes += total;
        currentEntries += 2;
    }

    private void newOutputStream() throws IOException {
        String canonicalPath = constructorFile.getCanonicalPath();
        String path = canonicalPath;
        synchronized (outputMutex) {
            // use the constructor filename for the first zip,
            // then add filecount to subsequent archives, if any.
            if (fileCount > 0) {
                if (path.endsWith(EXTENSION)) {
                    String pathPattern = "(.+)" + EXTENSION + "$";
                    // one MILLION zip files...
                    String replacementPattern = "$1-"
                            + String.format("%06d", fileCount)
                            + EXTENSION;
                    path = path.replaceFirst(pathPattern,
                            replacementPattern);
                } else {
                    path = path + "." + fileCount;
                }
                logger.fine("built " + path + " from " + canonicalPath);
                assert path.equals(canonicalPath);
            }
            logger.info("new output package " + path);
            // TODO this flush can take several seconds
            if (outputStream != null) {
                flush();
                close();
            }
            currentFileBytes = 0;
            currentEntries = 0;
            currentFile = new File(path);
            outputStream = new ZipOutputStream(new FileOutputStream(
                    currentFile));
            fileCount++;
        }
    }

    public File getCurrentFile() {
        return currentFile;
    }

}
